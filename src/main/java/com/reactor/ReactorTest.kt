package com.reactor

import org.junit.jupiter.api.Test
import org.reactivestreams.Subscription
import reactor.core.publisher.BaseSubscriber
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.SynchronousSink
import reactor.util.function.Tuple2
import reactor.util.function.Tuples
import java.time.Duration

class ReactorTest {

    val listFox = listOf("the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog")
    val listSphinx = listOf("sphinx", "of", "black", "quartz", "judge", "my", "vow")

    // Basic subscriber implementation
    fun createBaseSubscriber() : BaseSubscriber<Any> {
        return object : BaseSubscriber<Any>() {
            override fun hookOnComplete() {
                log("onComplete", null)
            }

            override fun hookOnNext(value: Any) {
                log("onNext", value)
            }

            override fun hookOnSubscribe(subscription: Subscription) {
                log("onSubscriber in baseSubscriber", null)
                super.hookOnSubscribe(subscription)
            }
        }
    }

    fun log(event : String, elem : Any?) {
        val event = "Event = $event"
        val printedElem = if (elem != null) "Value = $elem" else ""
        println("${Thread.currentThread().name}: $event $printedElem")
    }

    /**
     * Publishers subscribe to each other through a chain of subscriptions
     *
     * Each newly created subscription in the publishers chain has InnerOperator type which is both
     * Subscriber and Subscription
     *
     * publisher4.subscribe(baseSubscriber) -> publisher3.subscribe(newSubscription1)
     *          -> publisher2.subscribe(newSubscription2) ->  publisher1.subscribe(newSubscription3)
     *
     * After subscribing to "publisher1" a chain of onSubscribe is propagated up until "baseSubscriber" through
     * the onSubscribe() method
     * newSubscription3.onSubscribe() -> newSubscription2.onSubscribe()
     *          -> newSubscription1.onSubscribe() -> baseSubscriber.onSubscribe()
     *
     * baseSubscriber.onSubscribe() requests unlimited data from the chain
     *
     * baseSubscriber.request() -> subscription1.request()
     *          -> subscription2.request() -> subscription3.request()
     *
     * onNext() method in each InnerOperator is propagated from newSbuscription3 to baseSubscriber
     */
    @Test
    fun createSimpleStream() {
        // Publishers are immutable, a new publisher is created upon each operation in the chain
        val publisher1 = Flux.fromIterable(listFox)
        val publisher2 = publisher1.map(String::toUpperCase)
        val publisher3 = publisher2.distinct()
        val publisher4 = publisher3.filter { el -> el.length > 1 }

        publisher4.subscribe(createBaseSubscriber())

        /**
         * Reactor doesn't impose a particular threading model here
         * If the data source is available immediately, then the data will flow on the main thread
         *
         *  When the data is readily available from memory, Reactor acts in simple push mode like a typical Java Stream
         */
        log("Execution finished", null)
    }

    /**
     * FlatMap:
     * Transform the elements into Publishers -> flatten the created Publishers
     *
     * Zip:
     * Zips the Flux with another Publisher source, wait for both to emit an element and combine
     * using a Combinator function
     *
     * ConcatWith:
     * Concatenate original Publisher with the provided Publisher
     */
    @Test
    fun flatMapAndZipStream() {
        val publisher1 = Flux.fromIterable(listFox)
                                .doOnNext { println("${Thread.currentThread().name}: $it") }
                                .concatWith(Flux.just("nicely"))
                                .flatMap { Flux.fromArray(it.toCharArray().toTypedArray()) }
                                .zipWith(Flux.range(0, 50)) { symbol, index -> "$index : $symbol" }
                                .doOnComplete { log("onComplete InnerOperator", null) }

        publisher1.subscribe(createBaseSubscriber())
    }

    /**
     * The main thread, as expected, executes up until the delayElements() operation is invoked
     * Instead of blocking on the delay the main thread doesn't block and exits the function
     *
     * Thread.sleep() needs to be invoked to let the Stream finish
     */
    @Test
    fun delayedStream() {
        val flatMapAndZipStream = Flux.fromIterable(listFox)
            .doOnNext { log("onNext InnerOperator", it) }
            .concatWith(Flux.just("nicely"))
            .delayElements(Duration.ofSeconds(1))
            .flatMap { Flux.fromArray(it.toCharArray().toTypedArray()) }
            .zipWith(Flux.range(0, 50)) { symbol, index -> "$index : $symbol" }
            .doOnComplete { log("onComplete InnerOperator", null) }

        flatMapAndZipStream.subscribe(createBaseSubscriber())

        Thread.sleep(60_000);

        log("Exiting the main function", null)
    }

    @Test
    fun delayedStreamBlocking() {
        val flatMapAndZipStream = Flux.fromIterable(listFox)
            .doOnNext { log("onNext InnerOperator", it) }
            .concatWith(Flux.just("nicely"))
            .delayElements(Duration.ofSeconds(1))
            .flatMap { Flux.fromArray(it.toCharArray().toTypedArray()) }
            .zipWith(Flux.range(0, 50)) { symbol, index -> "$index : $symbol" }
            .doOnComplete { log("onComplete InnerOperator", null) }

        flatMapAndZipStream.subscribe(createBaseSubscriber())

        Thread.sleep(60_000);

        log("Exiting the main function", null)
    }

    @Test
    fun concatCollectStream() {
        Flux.concat(Mono.just(1), Mono.just(2))
            .collectSortedList(reverseOrder())
            .subscribe { println(it.javaClass) };
    }

    @Test
    fun generateSinkStream() {
        var counter = 0;

        var publisher1 = Flux.generate<Int> { ss -> if (counter < 100) ss.next(counter++) else ss.complete() }
        var publisher2 = Flux.generate(
            { Tuples.of(0L, 1L) },
            { state: Tuple2<Long, Long>, sink: SynchronousSink<Long> ->
                if (state.t2 < 100) sink.next(state.t2) else sink.complete()
                val newValue = state.t1 + state.t2
                Tuples.of(state.t2, newValue)
            })


        log("invoke pubsliher1", null)
        publisher1.subscribe(createBaseSubscriber())

        log("invoke pubsliher2", null)
        publisher2.subscribe(createBaseSubscriber())
    }

    /**
     * usingWhen allows to manage resources in the reactive way by subscribing to the instance of Publisher
     * Example
     * Flux.usingWhen(
     * Transaction.beginTransaction(), // Get a Mono of Transaction
     * transaction -> transaction.insertRows(Flux.just("A", "B", "C")),Transaction::commit, // Get Transaction instance
     *                                                                                      // and perform an operation
     * Transaction::commit //Commit on success
     * Transaction::rollback //Rollback on fail
     *
     * Each lambda returns Publisher for the sake of asynchrony
     */
    @Test
    fun usingWhenStream() {
        val ioRequestSynchronousPublisher = Flux.using(
            { Connection() },
            { connection ->
                Flux.fromIterable(
                    connection.getData()
                )
            },
            { conn -> conn.close() }
        )

        val ioRequestAsynchronousSynchronousPublisher = Flux.usingWhen(Flux.just("one"), { s : String -> Flux.just(s.split("")) }, { resource -> Flux.just(resource) } )

        ioRequestSynchronousPublisher.subscribe(createBaseSubscriber())
    }
}


class Connection {
    fun newConnection() : Connection {
        return Connection()
    }
    fun getData() : List<String> {
        return listOf("One", "Two", "Three")
    }
    fun close() {
        println("Closing connection")
    }
}











