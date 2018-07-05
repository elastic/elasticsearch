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

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.script.Script.DEFAULT_TEMPLATE_LANG;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DateIndexNameProcessorTests extends ESTestCase {

    public void testJodaPattern() throws Exception {
        Function<String, DateTime> function = DateFormat.Joda.getFunction("yyyy-MM-dd'T'HH:mm:ss.SSSZ", DateTimeZone.UTC, Locale.ROOT);
        DateIndexNameProcessor processor = new DateIndexNameProcessor(
                "_tag", "_field", Collections.singletonList(function), DateTimeZone.UTC,
                "events-", "y", "yyyyMMdd", TestTemplateService.instance()
        );

        IngestDocument document = new IngestDocument("_index", "_type", "_id", null, null, null,
                Collections.singletonMap("_field", "2016-04-25T12:24:20.101Z"));
        processor.execute(document);
        assertThat(document.getSourceAndMetadata().get("_index"), equalTo("<events-{20160425||/y{yyyyMMdd|UTC}}>"));
    }

    public void testTAI64N()throws Exception {
        Function<String, DateTime> function = DateFormat.Tai64n.getFunction(null, DateTimeZone.UTC, null);
        DateIndexNameProcessor dateProcessor = new DateIndexNameProcessor("_tag", "_field", Collections.singletonList(function),
                DateTimeZone.UTC, "events-", "m", "yyyyMMdd", TestTemplateService.instance());
        IngestDocument document = new IngestDocument("_index", "_type", "_id", null, null, null,
                Collections.singletonMap("_field", (randomBoolean() ? "@" : "") + "4000000050d506482dbdf024"));
        dateProcessor.execute(document);
        assertThat(document.getSourceAndMetadata().get("_index"), equalTo("<events-{20121222||/m{yyyyMMdd|UTC}}>"));
    }

    public void testUnixMs()throws Exception {
        Function<String, DateTime> function = DateFormat.UnixMs.getFunction(null, DateTimeZone.UTC, null);
        DateIndexNameProcessor dateProcessor = new DateIndexNameProcessor("_tag", "_field", Collections.singletonList(function),
                DateTimeZone.UTC, "events-", "m", "yyyyMMdd", TestTemplateService.instance());
        IngestDocument document = new IngestDocument("_index", "_type", "_id", null, null, null,
                Collections.singletonMap("_field", "1000500"));
        dateProcessor.execute(document);
        assertThat(document.getSourceAndMetadata().get("_index"), equalTo("<events-{19700101||/m{yyyyMMdd|UTC}}>"));

        document = new IngestDocument("_index", "_type", "_id", null, null, null,
                Collections.singletonMap("_field", 1000500L));
        dateProcessor.execute(document);
        assertThat(document.getSourceAndMetadata().get("_index"), equalTo("<events-{19700101||/m{yyyyMMdd|UTC}}>"));
    }

    public void testUnix()throws Exception {
        Function<String, DateTime> function = DateFormat.Unix.getFunction(null, DateTimeZone.UTC, null);
        DateIndexNameProcessor dateProcessor = new DateIndexNameProcessor("_tag", "_field", Collections.singletonList(function),
                DateTimeZone.UTC, "events-", "m", "yyyyMMdd", TestTemplateService.instance());
        IngestDocument document = new IngestDocument("_index", "_type", "_id", null, null, null,
                Collections.singletonMap("_field", "1000.5"));
        dateProcessor.execute(document);
        assertThat(document.getSourceAndMetadata().get("_index"), equalTo("<events-{19700101||/m{yyyyMMdd|UTC}}>"));
    }

    public void testTemplateSupported() throws Exception {
        ScriptService scriptService = mock(ScriptService.class);
        TestTemplateService.MockTemplateScript.Factory factory = new TestTemplateService.MockTemplateScript.Factory("script_result");
        when(scriptService.compile(any(Script.class), Matchers.<ScriptContext<TemplateScript.Factory>>any())).thenReturn(factory);
        when(scriptService.isLangSupported(DEFAULT_TEMPLATE_LANG)).thenReturn(true);

        DateIndexNameProcessor dateProcessor = new DateIndexNameProcessor("_tag", "_field",
            Collections.singletonList(DateFormat.Unix.getFunction(null, DateTimeZone.UTC, null)),
            DateTimeZone.UTC, "events-", "m", "yyyyMMdd", scriptService);
        IngestDocument document = new IngestDocument("_index", "_type", "_id", null, null, null,
            Collections.singletonMap("_field", "1000.5"));
        dateProcessor.execute(document);

        // here we only care that the script was compiled and that it returned what we expect.
        Mockito.verify(scriptService).compile(any(Script.class), Matchers.<ScriptContext<TemplateScript.Factory>>any());
        assertThat(document.getSourceAndMetadata().get("_index"), equalTo("script_result"));
    }
}
