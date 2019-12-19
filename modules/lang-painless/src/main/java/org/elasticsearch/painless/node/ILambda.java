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

package org.elasticsearch.painless.node;

import java.util.List;

/**
 * Interface for lambda/method reference nodes. They need special handling by LDefCall.
 * <p>
 * This is because they know nothing about the target interface, and can only push
 * all their captures onto the stack and defer everything until link-time.
 */
interface ILambda {

    /** Returns reference to resolve at link-time */
    String getPointer();

    /** Returns the types of captured parameters. Can be empty */
    List<Class<?>> getCaptures();

    /** Returns the number of captured parameters */
    default int getCaptureCount() {
        return getCaptures().size();
    }
}
