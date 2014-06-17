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
package io.netty.channel.nio;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;

import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.Executor;
import io.netty.util.concurrent.DefaultExecutorFactory;
import io.netty.util.concurrent.ExecutorFactory;

/**
 * {@link MultithreadEventLoopGroup} implementations which is used for NIO {@link Selector} based {@link Channel}s.
 */
public class NioEventLoopGroup extends MultithreadEventLoopGroup {

    /**
     * Create a new instance using the default number of {@link EventLoop}s, the default {@link Executor} and
     * the {@link SelectorProvider} which is returned by {@link SelectorProvider#provider()}.
     *
     * @see DefaultExecutorFactory
     */
    public NioEventLoopGroup() {
        this(0);
    }

    public NioEventLoopGroup(int nEventLoops) {
        this(nEventLoops, (Executor) null);
    }

    public NioEventLoopGroup(int nEventLoops, Executor executor) {
        this(nEventLoops, executor, SelectorProvider.provider());
    }

    public NioEventLoopGroup(int nEventLoops, ExecutorFactory executorFactory) {
        this(nEventLoops, executorFactory, SelectorProvider.provider());
    }

    public NioEventLoopGroup(int nEventLoops, Executor executor, final SelectorProvider selectorProvider) {
        super(nEventLoops, executor, selectorProvider);
    }

    public NioEventLoopGroup(
            int nEventLoops, ExecutorFactory executorFactory, final SelectorProvider selectorProvider) {
        super(nEventLoops, executorFactory, selectorProvider);
    }

    /**
     * Sets the percentage of the desired amount of time spent for I/O in the child event loops.  The default value is
     * {@code 50}, which means the event loop will try to spend the same amount of time for I/O as for non-I/O tasks.
     */
    public void setIoRatio(int ioRatio) {
        for (EventExecutor e: children()) {
            ((NioEventLoop) e).setIoRatio(ioRatio);
        }
    }

    /**
     * Replaces the current {@link Selector}s of the child event loops with newly created {@link Selector}s to work
     * around the  infamous epoll 100% CPU bug.
     */
    public void rebuildSelectors() {
        for (EventExecutor e: children()) {
            ((NioEventLoop) e).rebuildSelector();
        }
    }

    @Override
    protected EventLoop newChild(Executor executor, Object... args) throws Exception {
        return new NioEventLoop(this, executor, (SelectorProvider) args[0]);
    }
}
