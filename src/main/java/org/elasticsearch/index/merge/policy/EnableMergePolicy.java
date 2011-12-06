/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.merge.policy;

/**
 * Allows to control if merge should be enabled on the current thread or not. Defaults to
 * not being enabled.
 * <p/>
 * <p>This allows us to disable merging for things like adding docs or refresh (which might block
 * if no threads are there to handle the merge) and do it on flush (for example) or on explicit API call.
 *
 *
 */
public interface EnableMergePolicy {

    boolean isMergeEnabled();

    void enableMerge();

    void disableMerge();
}