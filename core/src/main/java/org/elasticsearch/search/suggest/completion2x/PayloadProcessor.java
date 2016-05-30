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

package org.elasticsearch.search.suggest.completion2x;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import java.io.IOException;

interface PayloadProcessor {

    BytesRef buildPayload(BytesRef surfaceForm, long weight, BytesRef payload) throws IOException;

    void parsePayload(BytesRef payload, SuggestPayload ref) throws IOException;

    static class SuggestPayload {
        final BytesRefBuilder payload = new BytesRefBuilder();
        long weight = 0;
        final BytesRefBuilder surfaceForm = new BytesRefBuilder();
    }
}
