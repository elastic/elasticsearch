/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.index.indexing;

import org.elasticsearch.index.engine.Engine;

/**
 * An indexing listener for indexing, delete, events.
 */
public abstract class IndexingOperationListener {

    /**
     * Called before the indexing occurs.
     */
    public Engine.Create preCreate(Engine.Create create) {
        return create;
    }

    /**
     * Called after the indexing occurs, under a locking scheme to maintain
     * concurrent updates to the same doc.
     * <p/>
     * Note, long operations should not occur under this callback.
     */
    public void postCreateUnderLock(Engine.Create create) {

    }

    /**
     * Called after the indexing operation occurred.
     */
    public void postCreate(Engine.Create create) {

    }

    /**
     * Called before the indexing occurs.
     */
    public Engine.Index preIndex(Engine.Index index) {
        return index;
    }

    /**
     * Called after the indexing occurs, under a locking scheme to maintain
     * concurrent updates to the same doc.
     * <p/>
     * Note, long operations should not occur under this callback.
     */
    public void postIndexUnderLock(Engine.Index index) {

    }

    /**
     * Called after the indexing operation occurred.
     */
    public void postIndex(Engine.Index index) {

    }

    /**
     * Called before the delete occurs.
     */
    public Engine.Delete preDelete(Engine.Delete delete) {
        return delete;
    }

    /**
     * Called after the delete occurs, under a locking scheme to maintain
     * concurrent updates to the same doc.
     * <p/>
     * Note, long operations should not occur under this callback.
     */
    public void postDeleteUnderLock(Engine.Delete delete) {

    }

    /**
     * Called after the delete operation occurred.
     */
    public void postDelete(Engine.Delete delete) {

    }

    public Engine.DeleteByQuery preDeleteByQuery(Engine.DeleteByQuery deleteByQuery) {
        return deleteByQuery;
    }

    public void postDeleteByQuery(Engine.DeleteByQuery deleteByQuery) {

    }
}
