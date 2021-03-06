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

    private ConcurrentHashMap<String, Integer> threadProcessingCount = new ConcurrentHashMap<>();

    /**
     * Test fails.
     * Expected behavior: processing is evenly distributed accross 4 threads.
     * Example distribution for one test run: [801, 73, 71, 55]
     */
    @Test
    public void groupByWithParallel() {

        // GIVEN
        Flowable<Integer> flowableWithGroupByAndParallel = Flowable.just(1, 2, 3, 4)
                .repeat(250)
                .groupBy(number -> number % MAX_NUMBER_OF_THREADS)
                .parallel()
                .runOn(Schedulers.io())
                .flatMap(group -> {
                    System.out.println("Mapping on thead " + currentThread().getName());
                    return group.doOnNext(number -> {
                        printThreadAndNumber(number);
                        increaseThreadProcessingCount(threadProcessingCount);
                    });
                })
                .sequential();

        // WHEN
        flowableWithGroupByAndParallel
                .test()
                .awaitTerminalEvent();

        // THEN
        System.out.println(threadProcessingCount);
        assertThat(threadProcessingCount.values()).containsExactly(250, 250, 250, 250);

    }

    /**
     * Test fails.
     * Expected behavior: processing is evenly distributed accross 4 threads.
     * Example distribution for one test run: [801, 73, 71, 55]
     */
    @Test
    public void flatMapWithSubscribeOn() {

        // GIVEN
        Flowable<Integer> flowableWithFlapMapAndSubscribeOn = Flowable.just(1, 2, 3, 4)
                .repeat(250)
                .groupBy(number -> number % MAX_NUMBER_OF_THREADS)
                .flatMap(group -> group.subscribeOn(Schedulers.io()).doOnNext(number -> {
                    printThreadAndNumber(number);
                    increaseThreadProcessingCount(threadProcessingCount);
                }));

        // WHEN
        flowableWithFlapMapAndSubscribeOn
                .test()
                .awaitTerminalEvent();

        // THEN
        assertThat(threadProcessingCount.values()).containsExactly(250, 250, 250, 250);

    }

    /**
     * Test succeeds.
     */
    @Test
    public void flatMapWithObserveOn() {

        // GIVEN
        Flowable<Integer> flowableWithFlapMapAndObserveOn = Flowable.just(1, 2, 3, 4)
                .repeat(250)
                .groupBy(number -> number % MAX_NUMBER_OF_THREADS)
                .flatMap(group -> group.observeOn(Schedulers.io()).doOnNext(number -> {
                    printThreadAndNumber(number);
                    increaseThreadProcessingCount(threadProcessingCount);
                }));

        // WHEN
        flowableWithFlapMapAndObserveOn
                .test()
                .awaitTerminalEvent();

        // THEN
        assertThat(threadProcessingCount.values()).containsExactly(250, 250, 250, 250);

    }

    private static void printThreadAndNumber(Integer number) {
        System.out.println(Thread.currentThread().getName() + " " + number);
    }

    private static void increaseThreadProcessingCount(ConcurrentHashMap<String, Integer> threadProcessingCount) {
        threadProcessingCount
                .compute(currentThread().getName(),
                        (thread, count) -> ofNullable(count).map(previousCount -> ++previousCount).orElse(1));
    }
}
