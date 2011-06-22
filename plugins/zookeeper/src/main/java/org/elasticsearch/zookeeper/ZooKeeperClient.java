/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.zookeeper;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.LifecycleComponent;

import java.util.Set;

/**
 * @author imotov
 */
public interface ZooKeeperClient extends LifecycleComponent<ZooKeeperClient> {

    void createPersistentNode(String path) throws InterruptedException;

    void setOrCreatePersistentNode(String path, byte[] data) throws InterruptedException;

    byte[] getOrCreateTransientNode(String path, byte[] data, NodeListener nodeListener) throws InterruptedException;

    void setOrCreateTransientNode(String path, byte[] data) throws InterruptedException;


    byte[] getNode(final String path, @Nullable final NodeListener nodeListener) throws InterruptedException;

    Set<String> listNodes(String path, NodeListChangedListener nodeListChangedListener) throws InterruptedException;

    void deleteNode(String path) throws InterruptedException;

    void addSessionResetListener(SessionResetListener sessionResetListener);

    void removeSessionResetListener(SessionResetListener sessionResetListener);

    boolean connected();

    long sessionId();

    interface SessionResetListener {
        public void sessionReset();
    }

    interface NodeListener {
        public void onNodeCreated(String id);

        public void onNodeDeleted(String id);

        public void onNodeDataChanged(String id);
    }

    interface NodeListChangedListener {
        public void onNodeListChanged();
    }
}
