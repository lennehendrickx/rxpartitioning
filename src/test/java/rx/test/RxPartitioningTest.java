package rx.test;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Thread.currentThread;
import static java.util.Optional.ofNullable;
import static org.assertj.core.api.Assertions.assertThat;

public class RxPartitioningTest {

    private static final int MAX_NUMBER_OF_THREADS = 4;

    /**
     * Test fails.
     * Expected behavior: processing is evenly distributed accross 4 threads.
     * Example distribution for one test run: [801, 73, 71, 55]
     */
    @Test
    public void groupByWithParallel() {

        ConcurrentHashMap<String, Integer> threadProcessingCount = new ConcurrentHashMap<>();

        // GIVEN
        Flowable<Integer> flowableWithGroupByAndParallel = Flowable.just(1, 2, 3, 4)
                .repeat(250)
                .groupBy(number -> number % MAX_NUMBER_OF_THREADS)
                .parallel()
                .runOn(Schedulers.io())
                .flatMap(group -> group.doOnNext(number -> {
                    printThreadAnNumber(number);
                    increaseThreadProcessingCount(threadProcessingCount);
                }))
                .sequential();

        // WHEN
        flowableWithGroupByAndParallel
                .test()
                .awaitTerminalEvent();

        // THEN
        assertThat(threadProcessingCount.values()).containsExactly(250, 250, 250, 250);

    }

    /**
     * Test fails.
     * Expected behavior: processing is evenly distributed accross 4 threads.
     * Example distribution for one test run: [801, 73, 71, 55]
     */
    @Test
    public void flatMapWithSubscribeOn() {

        ConcurrentHashMap<String, Integer> threadCallCount = new ConcurrentHashMap<>();

        // GIVEN
        Flowable<Integer> flowableWithFlapMapAndObserveOn = Flowable.just(1, 2, 3, 4)
                .repeat(250)
                .groupBy(number -> number % MAX_NUMBER_OF_THREADS)
                .flatMap(group -> group.subscribeOn(Schedulers.io()).doOnNext(number -> {
                    printThreadAnNumber(number);
                    increaseThreadProcessingCount(threadCallCount);
                }));

        // WHEN
        flowableWithFlapMapAndObserveOn
                .test()
                .awaitTerminalEvent();

        // THEN
        assertThat(threadCallCount.values()).containsExactly(250, 250, 250, 250);

    }

    /**
     * Test succeeds.
     */
    @Test
    public void flatMapWithObserveOn() {

        ConcurrentHashMap<String, Integer> threadCallCount = new ConcurrentHashMap<>();

        // GIVEN
        Flowable<Integer> flowableWithFlapMapAndObserveOn = Flowable.just(1, 2, 3, 4)
                .repeat(250)
                .groupBy(number -> number % MAX_NUMBER_OF_THREADS)
                .flatMap(group -> group.observeOn(Schedulers.io()).doOnNext(number -> {
                    printThreadAnNumber(number);
                    increaseThreadProcessingCount(threadCallCount);
                }));

        // WHEN
        flowableWithFlapMapAndObserveOn
                .test()
                .awaitTerminalEvent();

        // THEN
        assertThat(threadCallCount.values()).containsExactly(250, 250, 250, 250);

    }

    private static void printThreadAnNumber(Integer number) {
        System.out.println(Thread.currentThread().getName() + " " + number);
    }

    private static void increaseThreadProcessingCount(ConcurrentHashMap<String, Integer> threadCallCount) {
            threadCallCount
                    .compute(currentThread().getName(),
                            (thread, count) -> ofNullable(count).map(previousCount -> ++previousCount).orElse(1));
    }
}
