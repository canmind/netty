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
package io.netty.channel;

import io.netty.util.concurrent.ExecutorFactory;

import java.util.concurrent.Executor;

/**
 * {@link MultithreadEventLoopGroup} which must be used for the local transport.
 */
public class DefaultEventLoopGroup extends MultithreadEventLoopGroup {

    /**
     * Create a new instance with the default number of threads.
     */
    public DefaultEventLoopGroup() {
        this(0);
    }

    /**
     * Create a new instance
     *
     * @param nEventLoops   the number of {@link EventLoop}s to use.
     */
    public DefaultEventLoopGroup(int nEventLoops) {
        this(nEventLoops, (Executor) null);
    }

    /**
     * Create a new instance
     *
     * @param nEventLoops   the number of {@link EventLoop}s ({@link #children}) that will be used by this instance.
     * @param executor      the Executor to use, or {@code null} if the default should be used.
     */
    public DefaultEventLoopGroup(int nEventLoops, Executor executor) {
        super(nEventLoops, executor);
    }

    public DefaultEventLoopGroup(int nEventLoops, ExecutorFactory executorFactory) {
        super(nEventLoops, executorFactory);
    }

    @Override
    protected EventLoop newChild(Executor executor, Object... args) throws Exception {
        return new DefaultEventLoop(this, executor);
    }
}
