/*
 * Copyright 2014 The Netty Project
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
package io.netty.channel.epoll;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ExecutorFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 * {@link EventLoopGroup} which uses epoll under the covers. Because of this
 * it only works on linux.
 */
public final class EpollEventLoopGroup extends MultithreadEventLoopGroup {

    public EpollEventLoopGroup() {
        this(0);
    }

    public EpollEventLoopGroup(int nEventLoops) {
        this(nEventLoops, (Executor) null);
    }

    public EpollEventLoopGroup(int nEventLoops, Executor executor) {
        this(nEventLoops, executor, 128);
    }

    public EpollEventLoopGroup(int nEventLoops, ExecutorFactory executorFactory) {
        this(nEventLoops, executorFactory, 128);
    }

    /**
     * and the given
     * maximal amount of epoll events to handle per epollWait(...).
     */
    public EpollEventLoopGroup(int nEventLoops, Executor executor, int maxEventsAtOnce) {
        super(nEventLoops, executor, maxEventsAtOnce);
    }

    /**
     * and the given
     * maximal amount of epoll events to handle per epollWait(...).
     */
    public EpollEventLoopGroup(int nEventLoops, ExecutorFactory executorFactory, int maxEventsAtOnce) {
        super(nEventLoops, executorFactory, maxEventsAtOnce);
    }

    /**
     * Sets the percentage of the desired amount of time spent for I/O in the child event loops.  The default value is
     * {@code 50}, which means the event loop will try to spend the same amount of time for I/O as for non-I/O tasks.
     */
    public void setIoRatio(int ioRatio) {
        for (EventExecutor e: children()) {
            ((EpollEventLoop) e).setIoRatio(ioRatio);
        }
    }

    @Override
    protected EventLoop newChild(Executor executor, Object... args) throws Exception {
        return new EpollEventLoop(this, executor, (Integer) args[0]);
    }
}
