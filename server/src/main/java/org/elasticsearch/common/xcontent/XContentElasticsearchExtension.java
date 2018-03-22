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

package org.elasticsearch.common.xcontent;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTimeZone;
import org.joda.time.tz.CachedDateTimeZone;
import org.joda.time.tz.FixedDateTimeZone;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * SPI extensions for Elasticsearch-specific classes (like the Lucene or Joda
 * dependency classes) that need to be encoded by {@link XContentBuilder} in a
 * specific way.
 */
public class XContentElasticsearchExtension implements XContentBuilderExtension {

    @Override
    public Map<Class<?>, XContentBuilder.Writer> getXContentWriters() {
        Map<Class<?>, XContentBuilder.Writer> writers = new HashMap<>();

        // Fully-qualified here to reduce ambiguity around our (ES') Version class
        writers.put(org.apache.lucene.util.Version.class, (b, v) -> b.value(Objects.toString(v)));
        writers.put(DateTimeZone.class, (b, v) -> b.value(Objects.toString(v)));
        writers.put(CachedDateTimeZone.class, (b, v) -> b.value(Objects.toString(v)));
        writers.put(FixedDateTimeZone.class, (b, v) -> b.value(Objects.toString(v)));

        writers.put(BytesReference.class, (b, v) -> {
            if (v == null) {
                b.nullValue();
            } else {
                BytesRef bytes = ((BytesReference) v).toBytesRef();
                b.value(bytes.bytes, bytes.offset, bytes.length);
            }
        });

        writers.put(BytesRef.class, (b, v) -> {
            if (v == null) {
                b.nullValue();
            } else {
                BytesRef bytes = (BytesRef) v;
                b.value(bytes.bytes, bytes.offset, bytes.length);
            }
        });
        return writers;
    }

    @Override
    public Map<Class<?>, XContentBuilder.HumanReadableTransformer> getXContentHumanReadableTransformers() {
        Map<Class<?>, XContentBuilder.HumanReadableTransformer> transformers = new HashMap<>();
        transformers.put(TimeValue.class, v -> ((TimeValue) v).millis());
        transformers.put(ByteSizeValue.class, v -> ((ByteSizeValue) v).getBytes());
        return transformers;
    }
}
