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
package org.elasticsearch.ingest.common;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;

public class DateIndexNameProcessorTests extends ESTestCase {

    public void testJavaPattern() throws Exception {
        Function<String, ZonedDateTime> function = DateFormat.Java.getFunction("yyyy-MM-dd'T'HH:mm:ss.SSSXX", ZoneOffset.UTC, Locale.ROOT);
        DateIndexNameProcessor processor = createProcessor("_field", Collections.singletonList(function),
            ZoneOffset.UTC, "events-", "y", "yyyyMMdd");
        IngestDocument document = new IngestDocument("_index", "_id", null, null, null,
                Collections.singletonMap("_field", "2016-04-25T12:24:20.101Z"));
        processor.execute(document);
        assertThat(document.getSourceAndMetadata().get("_index"), equalTo("<events-{20160425||/y{yyyyMMdd|UTC}}>"));
    }

    public void testTAI64N()throws Exception {
        Function<String, ZonedDateTime> function = DateFormat.Tai64n.getFunction(null, ZoneOffset.UTC, null);
        DateIndexNameProcessor dateProcessor = createProcessor("_field", Collections.singletonList(function),
                ZoneOffset.UTC, "events-", "m", "yyyyMMdd");
        IngestDocument document = new IngestDocument("_index", "_id", null, null, null,
                Collections.singletonMap("_field", (randomBoolean() ? "@" : "") + "4000000050d506482dbdf024"));
        dateProcessor.execute(document);
        assertThat(document.getSourceAndMetadata().get("_index"), equalTo("<events-{20121222||/m{yyyyMMdd|UTC}}>"));
    }

    public void testUnixMs()throws Exception {
        Function<String, ZonedDateTime> function = DateFormat.UnixMs.getFunction(null, ZoneOffset.UTC, null);
        DateIndexNameProcessor dateProcessor = createProcessor("_field", Collections.singletonList(function),
                ZoneOffset.UTC, "events-", "m", "yyyyMMdd");
        IngestDocument document = new IngestDocument("_index", "_id", null, null, null,
                Collections.singletonMap("_field", "1000500"));
        dateProcessor.execute(document);
        assertThat(document.getSourceAndMetadata().get("_index"), equalTo("<events-{19700101||/m{yyyyMMdd|UTC}}>"));

        document = new IngestDocument("_index", "_id", null, null, null,
                Collections.singletonMap("_field", 1000500L));
        dateProcessor.execute(document);
        assertThat(document.getSourceAndMetadata().get("_index"), equalTo("<events-{19700101||/m{yyyyMMdd|UTC}}>"));
    }

    public void testUnix()throws Exception {
        Function<String, ZonedDateTime> function = DateFormat.Unix.getFunction(null, ZoneOffset.UTC, null);
        DateIndexNameProcessor dateProcessor = createProcessor("_field", Collections.singletonList(function),
                ZoneOffset.UTC, "events-", "m", "yyyyMMdd");
        IngestDocument document = new IngestDocument("_index", "_id", null, null, null,
                Collections.singletonMap("_field", "1000.5"));
        dateProcessor.execute(document);
        assertThat(document.getSourceAndMetadata().get("_index"), equalTo("<events-{19700101||/m{yyyyMMdd|UTC}}>"));
    }

    public void testTemplatedFields() throws Exception {
        String indexNamePrefix = randomAlphaOfLength(10);
        String dateRounding = randomFrom("y", "M", "w", "d", "h", "m", "s");
        String indexNameFormat = randomFrom("yyyy-MM-dd'T'HH:mm:ss.SSSZZ", "yyyyMMdd", "MM/dd/yyyy");
        String date = Integer.toString(randomInt());
        Function<String, ZonedDateTime> dateTimeFunction = DateFormat.Unix.getFunction(null, ZoneOffset.UTC, null);

        DateIndexNameProcessor dateProcessor = createProcessor("_field",
            Collections.singletonList(dateTimeFunction),  ZoneOffset.UTC, indexNamePrefix,
            dateRounding, indexNameFormat);

        IngestDocument document = new IngestDocument("_index", "_id", null, null, null,
            Collections.singletonMap("_field", date));
        dateProcessor.execute(document);

        assertThat(document.getSourceAndMetadata().get("_index"),
            equalTo("<"+indexNamePrefix+"{" + DateFormatter.forPattern(indexNameFormat)
                .format(dateTimeFunction.apply(date))+"||/"+dateRounding+"{"+indexNameFormat+"|UTC}}>"));
    }

    private DateIndexNameProcessor createProcessor(String field, List<Function<String, ZonedDateTime>> dateFormats,
                                                   ZoneId timezone, String indexNamePrefix, String dateRounding,
                                                   String indexNameFormat) {
        return new DateIndexNameProcessor(randomAlphaOfLength(10), null, field, dateFormats, timezone,
            new TestTemplateService.MockTemplateScript.Factory(indexNamePrefix),
            new TestTemplateService.MockTemplateScript.Factory(dateRounding),
            new TestTemplateService.MockTemplateScript.Factory(indexNameFormat)
        );
    }
}
