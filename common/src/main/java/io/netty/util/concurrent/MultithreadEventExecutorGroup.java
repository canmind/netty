/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util.concurrent;

import io.netty.util.metrics.EventExecutorChooser;
import io.netty.util.metrics.MetricsCollector;
import io.netty.util.metrics.NoMetricsCollector;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base class for {@link EventExecutorGroup} implementations that handles their tasks with multiple threads at
 * the same time.
 */
public abstract class MultithreadEventExecutorGroup<T extends EventExecutor> extends AbstractEventExecutorGroup {

    private final Set<EventExecutor> readonlyChildren;
    private final AtomicInteger terminatedChildren = new AtomicInteger();
    private final Promise<?> terminationFuture = new DefaultPromise(GlobalEventExecutor.INSTANCE);
    private final EventExecutorChooser<T> chooser;

    /**
     * Create a new instance.
     *
     * @param nEventExecutors   the number of {@link EventExecutor}s that will be used by this instance.
     *                             If {@code executor} is {@code null} this number will also be the parallelism
     *                             requested from the default executor. It is generally advised for the number
     *                             of {@link EventExecutor}s and the number of {@link Thread}s used by the
     *                             {@code executor} to lie very close together.
     * @param executorFactory   the {@link ExecutorFactory} to use, or {@code null} if the default should be used.
     * @param args                arguments which will passed to each {@link #newChild(Executor, Object...)} call
     */
    protected MultithreadEventExecutorGroup(int nEventExecutors, ExecutorFactory executorFactory,
                                             EventExecutorChooser<T> chooser, Object... args) {
        this(nEventExecutors, executorFactory == null ? null :
                                                         executorFactory.newExecutor(nEventExecutors),
              chooser, args);
    }

    /**
     * Create a new instance.
     *
     * @param nEventExecutors   the number of {@link EventExecutor}s that will be used by this instance.
     *                             If {@code executor} is {@code null} this number will also be the parallelism
     *                             requested from the default executor. It is generally advised for the number
     *                             of {@link EventExecutor}s and the number of {@link Thread}s used by the
     *                             {@code executor} to lie very close together.
     * @param executor           the {@link Executor} to use, or {@code null} if the default should be used.
     * @param args                arguments which will passed to each {@link #newChild(Executor, Object...)} call
     */
    protected MultithreadEventExecutorGroup(final int nEventExecutors, Executor executor,
                                             EventExecutorChooser<T> chooser, Object... args) {
        if (nEventExecutors <= 0) {
            throw new IllegalArgumentException(
                    String.format("nEventExecutors: %d (expected: > 0)", nEventExecutors));
        }

        if (executor == null) {
            executor = newDefaultExecutor(nEventExecutors);
        }

        this.chooser = chooser != null
                        ? chooser
                        : isPowerOfTwo(nEventExecutors)
                            ? new PowerOfTwoRoundRobinChooser(nEventExecutors)
                            : new RoundRobinChooser(nEventExecutors);

        for (int i = 0; i < nEventExecutors; i ++) {
            boolean success = false;
            try {
                MetricsCollector metrics = this.chooser.newMetricsCollector();
                T child = newChild(executor, metrics, args);
                metrics.init(child);
                this.chooser.addChild(child, metrics);

                success = true;
            } catch (Exception e) {
                // TODO: Think about if this is a good exception type
                throw new IllegalStateException("failed to create a child event loop", e);
            } finally {
                if (!success) {
                    for (EventExecutor e : this.chooser.children()) {
                        e.shutdownGracefully();
                    }

                    for (EventExecutor e : this.chooser.children()) {
                        try {
                            while (!e.isTerminated()) {
                                e.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
                            }
                        } catch (InterruptedException interrupted) {
                            // Let the caller handle the interruption.
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
        }

        final FutureListener<Object> terminationListener = new FutureListener<Object>() {
            @Override
            public void operationComplete(Future<Object> future) throws Exception {
                if (terminatedChildren.incrementAndGet() == nEventExecutors) {
                    terminationFuture.setSuccess(null);
                }
            }
        };

        for (EventExecutor e : this.chooser.children()) {
            e.terminationFuture().addListener(terminationListener);
        }

        Set<EventExecutor> childrenSet = new LinkedHashSet<EventExecutor>(this.chooser.children().size());
        childrenSet.addAll(this.chooser.children());
        readonlyChildren = Collections.unmodifiableSet(childrenSet);
    }

    protected Executor newDefaultExecutor(int nEventExecutors) {
        return new DefaultExecutorFactory(getClass()).newExecutor(nEventExecutors);
    }

    @Override
    public T next() {
        return chooser.next(null);
    }

    public T next(SocketAddress remoteAddress) {
        return chooser.next(remoteAddress);
    }

    /**
     * Return the number of {@link EventExecutor} this implementation uses. This number is the maps
     * 1:1 to the threads it use.
     */
    public final int executorCount() {
        return chooser.children().size();
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <E extends EventExecutor> Set<E> children() {
        return (Set<E>) readonlyChildren;
    }

    /**
     * Create a new EventExecutor which will later then accessible via the {@link #next()}  method. This method will be
     * called for each thread that will serve this {@link MultithreadEventExecutorGroup}.
     *
     */
    protected abstract T newChild(Executor executor, MetricsCollector metrics, Object... args) throws Exception;

    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
        for (EventExecutor l: chooser.children()) {
            l.shutdownGracefully(quietPeriod, timeout, unit);
        }
        return terminationFuture();
    }

    @Override
    public Future<?> terminationFuture() {
        return terminationFuture;
    }

    @Override
    @Deprecated
    public void shutdown() {
        for (EventExecutor l: chooser.children()) {
            l.shutdown();
        }
    }

    @Override
    public boolean isShuttingDown() {
        for (EventExecutor l: chooser.children()) {
            if (!l.isShuttingDown()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isShutdown() {
        for (EventExecutor l: chooser.children()) {
            if (!l.isShutdown()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isTerminated() {
        for (EventExecutor l: chooser.children()) {
            if (!l.isTerminated()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        loop: for (EventExecutor l: chooser.children()) {
            for (;;) {
                long timeLeft = deadline - System.nanoTime();
                if (timeLeft <= 0) {
                    break loop;
                }
                if (l.awaitTermination(timeLeft, TimeUnit.NANOSECONDS)) {
                    break;
                }
            }
        }
        return isTerminated();
    }

    private static boolean isPowerOfTwo(int val) {
        return (val & -val) == val;
    }

    private final class RoundRobinChooser implements EventExecutorChooser<T> {

        private final EventExecutor[] children;
        private final AtomicInteger index = new AtomicInteger();
        private int i;

        RoundRobinChooser(int nChildren) {
            children = new EventExecutor[nChildren];
        }

        @Override
        @SuppressWarnings("unchecked")
        public T next(SocketAddress remoteAddress) {
            // This cast is correct because children is only modified by addChild
            return (T) children[index.getAndIncrement() % children.length];
        }

        @Override
        public void addChild(T executor, MetricsCollector metrics) {
            children[i++] = executor;
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<T> children() {
            // This cast is correct because children only contains objects of type T
            // as is ensured by the addChild method
            return Arrays.asList((T[]) children);
        }

        @Override
        public MetricsCollector newMetricsCollector() {
            return NoMetricsCollector.INSTANCE;
        }
    }

    private final class PowerOfTwoRoundRobinChooser implements EventExecutorChooser<T> {

        private final EventExecutor[] children;
        private final AtomicInteger index = new AtomicInteger();
        private int i;

        PowerOfTwoRoundRobinChooser(int nChildren) {
            children = new EventExecutor[nChildren];
        }

        @Override
        @SuppressWarnings("unchecked")
        public T next(SocketAddress remoteAddress) {
            // This cast is correct because children is only modified by addChild
            return (T) children[index.getAndIncrement() & children.length - 1];
        }

        @Override
        public void addChild(T executor, MetricsCollector metrics) {
            children[i++] = executor;
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<T> children() {
            // This cast is correct because children only contains objects of type T
            // as is ensured by the addChild method
            return Arrays.asList((T[]) children);
        }

        @Override
        public MetricsCollector newMetricsCollector() {
            return NoMetricsCollector.INSTANCE;
        }
    }
}
