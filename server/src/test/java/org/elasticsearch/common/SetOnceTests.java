package org.elasticsearch.common;

import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;

public class SetOnceTests extends ESTestCase {

    public void testTrySet() {
        SetOnce<String> setOnce = new SetOnce<>();
        assertTrue(setOnce.trySet("1"));
        assertEquals("1", setOnce.get());

        assertFalse(setOnce.trySet("2"));
        assertEquals("1", setOnce.get());
    }

    public void testSet() {
        SetOnce<String> setOnce = new SetOnce<>();
        setOnce.set("1");
        assertEquals("1", setOnce.get());

        expectThrows(SetOnce.AlreadySetException.class, () -> setOnce.set("2"));
        assertEquals("1", setOnce.get());
    }

    public void testMultithreadedTrySet() throws InterruptedException {
        testMultithreaded(SetOnce::trySet);
    }

    public void testMultithreadedSet() throws InterruptedException {
        testMultithreaded((setOnce, threadNumber) -> {
            try {
                setOnce.set(threadNumber);
                return true;
            } catch (SetOnce.AlreadySetException e) {
                return false;
            }
        });
    }

    private void testMultithreaded(BiFunction<SetOnce<Integer>, Integer, Boolean> testMethod) throws InterruptedException {
        int threadCount = 100;
        CountDownLatch start = new CountDownLatch(threadCount);
        CountDownLatch end = new CountDownLatch(threadCount);
        SetOnce<Integer> setOnce = new SetOnce<>();
        AtomicInteger successes = new AtomicInteger();
        AtomicReference<Integer> successfulValue = new AtomicReference<>();
        for (int i = 0; i < 100; i++) {
            int threadNumber = i;
            new Thread(() -> {
                try {
                    start.countDown();
                    start.await();
                    if (testMethod.apply(setOnce, threadNumber)) {
                        successes.incrementAndGet();
                        successfulValue.set(threadNumber);
                    }
                    end.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }).start();
        }
        end.await();
        assertEquals(1, successes.get());
        assertEquals(successfulValue.get(), setOnce.get());
    }
}
