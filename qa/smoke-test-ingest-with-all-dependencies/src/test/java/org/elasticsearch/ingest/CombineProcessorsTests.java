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

package org.elasticsearch.ingest;

import com.maxmind.geoip2.DatabaseReader;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.ingest.core.CompoundProcessor;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Pipeline;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.ingest.geoip.GeoIpProcessor;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin;
import org.elasticsearch.ingest.grok.GrokProcessor;
import org.elasticsearch.ingest.grok.IngestGrokPlugin;
import org.elasticsearch.ingest.processor.AppendProcessor;
import org.elasticsearch.ingest.processor.ConvertProcessor;
import org.elasticsearch.ingest.processor.DateProcessor;
import org.elasticsearch.ingest.processor.ForEachProcessor;
import org.elasticsearch.ingest.processor.LowercaseProcessor;
import org.elasticsearch.ingest.processor.RemoveProcessor;
import org.elasticsearch.ingest.processor.RenameProcessor;
import org.elasticsearch.ingest.processor.SplitProcessor;
import org.elasticsearch.ingest.processor.TrimProcessor;
import org.elasticsearch.ingest.processor.UppercaseProcessor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CombineProcessorsTests extends ESTestCase {

    private static final String LOG = "70.193.17.92 - - [08/Sep/2014:02:54:42 +0000] \"GET /presentations/logstash-scale11x/images/ahhh___rage_face_by_samusmmx-d5g5zap.png HTTP/1.1\" 200 175208 \"http://mobile.rivals.com/board_posts.asp?SID=880&mid=198829575&fid=2208&tid=198829575&Team=&TeamId=&SiteId=\" \"Mozilla/5.0 (Linux; Android 4.2.2; VS980 4G Build/JDQ39B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.135 Mobile Safari/537.36\"";

    public void testLogging() throws Exception {
        Path configDir = createTempDir();
        Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        Files.copy(new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-City.mmdb")), geoIpConfigDir.resolve("GeoLite2-City.mmdb"));
        Map<String, DatabaseReader> databaseReaders = IngestGeoIpPlugin.loadDatabaseReaders(geoIpConfigDir);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "log");
        config.put("pattern", "%{COMBINEDAPACHELOG}");
        Processor processor1 = new GrokProcessor.Factory(IngestGrokPlugin.loadBuiltinPatterns()).doCreate(null, config);
        config = new HashMap<>();
        config.put("field", "response");
        config.put("type", "integer");
        Processor processor2 = new ConvertProcessor.Factory().create(config);
        config = new HashMap<>();
        config.put("field", "bytes");
        config.put("type", "integer");
        Processor processor3 = new ConvertProcessor.Factory().create(config);
        config = new HashMap<>();
        config.put("match_field", "timestamp");
        config.put("target_field", "timestamp");
        config.put("match_formats", Arrays.asList("dd/MMM/YYYY:HH:mm:ss Z"));
        Processor processor4 = new DateProcessor.Factory().create(config);
        config = new HashMap<>();
        config.put("source_field", "clientip");
        Processor processor5 = new GeoIpProcessor.Factory(databaseReaders).create(config);

        Pipeline pipeline = new Pipeline("_id", "_description", new CompoundProcessor(processor1, processor2, processor3, processor4, processor5));

        Map<String, Object> source = new HashMap<>();
        source.put("log", LOG);
        IngestDocument document = new IngestDocument("_index", "_type", "_id", null, null, null, null, source);
        pipeline.execute(document);

        assertThat(document.getSourceAndMetadata().size(), equalTo(17));
        assertThat(document.getSourceAndMetadata().get("request"), equalTo("/presentations/logstash-scale11x/images/ahhh___rage_face_by_samusmmx-d5g5zap.png"));
        assertThat(document.getSourceAndMetadata().get("agent"), equalTo("\"Mozilla/5.0 (Linux; Android 4.2.2; VS980 4G Build/JDQ39B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.135 Mobile Safari/537.36\""));
        assertThat(document.getSourceAndMetadata().get("auth"), equalTo("-"));
        assertThat(document.getSourceAndMetadata().get("ident"), equalTo("-"));
        assertThat(document.getSourceAndMetadata().get("verb"), equalTo("GET"));
        assertThat(document.getSourceAndMetadata().get("referrer"), equalTo("\"http://mobile.rivals.com/board_posts.asp?SID=880&mid=198829575&fid=2208&tid=198829575&Team=&TeamId=&SiteId=\""));
        assertThat(document.getSourceAndMetadata().get("response"), equalTo(200));
        assertThat(document.getSourceAndMetadata().get("bytes"), equalTo(175208));
        assertThat(document.getSourceAndMetadata().get("clientip"), equalTo("70.193.17.92"));
        assertThat(document.getSourceAndMetadata().get("httpversion"), equalTo("1.1"));
        assertThat(document.getSourceAndMetadata().get("rawrequest"), nullValue());
        assertThat(document.getSourceAndMetadata().get("timestamp"), equalTo("2014-09-08T02:54:42.000Z"));
        Map<String, Object> geoInfo = (Map<String, Object>) document.getSourceAndMetadata().get("geoip");
        assertThat(geoInfo.size(), equalTo(5));
        assertThat(geoInfo.get("continent_name"), equalTo("North America"));
        assertThat(geoInfo.get("city_name"), equalTo("Charlotte"));
        assertThat(geoInfo.get("country_iso_code"), equalTo("US"));
        assertThat(geoInfo.get("region_name"), equalTo("North Carolina"));
        assertThat(geoInfo.get("location"), notNullValue());
    }

    private static final String PERSON = "{\n" +
        "    \"age\": 33,\n" +
        "    \"eyeColor\": \"brown\",\n" +
        "    \"name\": \"Miranda Goodwin\",\n" +
        "    \"gender\": \"male\",\n" +
        "    \"company\": \"ATGEN\",\n" +
        "    \"email\": \"mirandagoodwin@atgen.com\",\n" +
        "    \"phone\": \"+1 (914) 489-3656\",\n" +
        "    \"address\": \"713 Bartlett Place, Accoville, Puerto Rico, 9221\",\n" +
        "    \"registered\": \"2014-11-23T08:34:21 -01:00\",\n" +
        "    \"tags\": [\n" +
        "      \"ex\",\n" +
        "      \"do\",\n" +
        "      \"occaecat\",\n" +
        "      \"reprehenderit\",\n" +
        "      \"anim\",\n" +
        "      \"laboris\",\n" +
        "      \"cillum\"\n" +
        "    ],\n" +
        "    \"friends\": [\n" +
        "      {\n" +
        "        \"id\": 0,\n" +
        "        \"name\": \"Wendi Odonnell\"\n" +
        "      },\n" +
        "      {\n" +
        "        \"id\": 1,\n" +
        "        \"name\": \"Mayra Boyd\"\n" +
        "      },\n" +
        "      {\n" +
        "        \"id\": 2,\n" +
        "        \"name\": \"Lee Gonzalez\"\n" +
        "      }\n" +
        "    ]\n" +
        "  }";

    @SuppressWarnings("unchecked")
    public void testMutate() throws Exception {
        ProcessorsRegistry.Builder builder = new ProcessorsRegistry.Builder();
        builder.registerProcessor("remove", (templateService, registry) -> new RemoveProcessor.Factory(templateService));
        builder.registerProcessor("trim", (templateService, registry) -> new TrimProcessor.Factory());
        ProcessorsRegistry registry = builder.build(TestTemplateService.instance());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "friends");
        Map<String, Object> removeConfig = new HashMap<>();
        removeConfig.put("field", "_value.id");
        config.put("processors", Collections.singletonList(Collections.singletonMap("remove", removeConfig)));
        ForEachProcessor processor1 = new ForEachProcessor.Factory(registry).create(config);
        config = new HashMap<>();
        config.put("field", "tags");
        config.put("value", "new_value");
        AppendProcessor processor2 = new AppendProcessor.Factory(TestTemplateService.instance()).create(config);
        config = new HashMap<>();
        config.put("field", "address");
        config.put("separator", ",");
        SplitProcessor processor3 = new SplitProcessor.Factory().create(config);
        config = new HashMap<>();
        config.put("field", "address");
        Map<String, Object> trimConfig = new HashMap<>();
        trimConfig.put("field", "_value");
        config.put("processors", Collections.singletonList(Collections.singletonMap("trim", trimConfig)));
        ForEachProcessor processor4 = new ForEachProcessor.Factory(registry).create(config);
        config = new HashMap<>();
        config.put("field", "company");
        LowercaseProcessor processor5 = new LowercaseProcessor.Factory().create(config);
        config = new HashMap<>();
        config.put("field", "gender");
        UppercaseProcessor processor6 = new UppercaseProcessor.Factory().create(config);
        config = new HashMap<>();
        config.put("field", "eyeColor");
        config.put("to", "eye_color");
        RenameProcessor processor7 = new RenameProcessor.Factory().create(config);
        Pipeline pipeline = new Pipeline("_id", "_description", new CompoundProcessor(
            processor1, processor2, processor3, processor4, processor5, processor6, processor7
        ));

        Map<String, Object> source = XContentHelper.createParser(new BytesArray(PERSON)).map();
        IngestDocument document = new IngestDocument("_index", "_type", "_id", null, null, null, null, source);
        pipeline.execute(document);

        assertThat(((List<Map<String, Object>>) document.getSourceAndMetadata().get("friends")).get(0).get("id"), nullValue());
        assertThat(((List<Map<String, Object>>) document.getSourceAndMetadata().get("friends")).get(1).get("id"), nullValue());
        assertThat(((List<Map<String, Object>>) document.getSourceAndMetadata().get("friends")).get(2).get("id"), nullValue());
        assertThat(document.getFieldValue("tags.7", String.class), equalTo("new_value"));

        List<String> addressDetails = document.getFieldValue("address", List.class);
        assertThat(addressDetails.size(), equalTo(4));
        assertThat(addressDetails.get(0), equalTo("713 Bartlett Place"));
        assertThat(addressDetails.get(1), equalTo("Accoville"));
        assertThat(addressDetails.get(2), equalTo("Puerto Rico"));
        assertThat(addressDetails.get(3), equalTo("9221"));

        assertThat(document.getSourceAndMetadata().get("company"), equalTo("atgen"));
        assertThat(document.getSourceAndMetadata().get("gender"), equalTo("MALE"));
        assertThat(document.getSourceAndMetadata().get("eye_color"), equalTo("brown"));
    }

}
