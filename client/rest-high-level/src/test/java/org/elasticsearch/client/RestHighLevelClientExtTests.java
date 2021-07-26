/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.Mockito.mock;

/**
 * This test works against a {@link RestHighLevelClient} subclass that simulates how custom response sections returned by
 * Elasticsearch plugins can be parsed using the high level client.
 */
public class RestHighLevelClientExtTests extends ESTestCase {

    private RestHighLevelClient restHighLevelClient;

    @Before
    public void initClient() {
        RestClient restClient = mock(RestClient.class);
        restHighLevelClient = new RestHighLevelClientExt(restClient);
    }

    public void testParseEntityCustomResponseSection() throws IOException {
        {
            HttpEntity jsonEntity = new NStringEntity("{\"custom1\":{ \"field\":\"value\"}}", ContentType.APPLICATION_JSON);
            BaseCustomResponseSection customSection = restHighLevelClient.parseEntity(jsonEntity, BaseCustomResponseSection::fromXContent);
            assertThat(customSection, instanceOf(CustomResponseSection1.class));
            CustomResponseSection1 customResponseSection1 = (CustomResponseSection1) customSection;
            assertEquals("value", customResponseSection1.value);
        }
        {
            HttpEntity jsonEntity = new NStringEntity("{\"custom2\":{ \"array\": [\"item1\", \"item2\"]}}", ContentType.APPLICATION_JSON);
            BaseCustomResponseSection customSection = restHighLevelClient.parseEntity(jsonEntity, BaseCustomResponseSection::fromXContent);
            assertThat(customSection, instanceOf(CustomResponseSection2.class));
            CustomResponseSection2 customResponseSection2 = (CustomResponseSection2) customSection;
            assertArrayEquals(new String[]{"item1", "item2"}, customResponseSection2.values);
        }
    }

    private static class RestHighLevelClientExt extends RestHighLevelClient {

        private RestHighLevelClientExt(RestClient restClient) {
            super(restClient, RestClient::close, getNamedXContentsExt());
        }

        private static List<NamedXContentRegistry.Entry> getNamedXContentsExt() {
            List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
            entries.add(new NamedXContentRegistry.Entry(BaseCustomResponseSection.class, new ParseField("custom1"),
                    CustomResponseSection1::fromXContent));
            entries.add(new NamedXContentRegistry.Entry(BaseCustomResponseSection.class, new ParseField("custom2"),
                    CustomResponseSection2::fromXContent));
            return entries;
        }
    }

    private abstract static class BaseCustomResponseSection {

        static BaseCustomResponseSection fromXContent(XContentParser parser) throws IOException {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            BaseCustomResponseSection custom = parser.namedObject(BaseCustomResponseSection.class, parser.currentName(), null);
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            return custom;
        }
    }

    private static class CustomResponseSection1 extends BaseCustomResponseSection {

        private final String value;

        private CustomResponseSection1(String value) {
            this.value = value;
        }

        static CustomResponseSection1 fromXContent(XContentParser parser) throws IOException {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("field", parser.currentName());
            assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
            CustomResponseSection1 responseSection1 = new CustomResponseSection1(parser.text());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            return responseSection1;
        }
    }

    private static class CustomResponseSection2 extends BaseCustomResponseSection {

        private final String[] values;

        private CustomResponseSection2(String[] values) {
            this.values = values;
        }

        static CustomResponseSection2 fromXContent(XContentParser parser) throws IOException {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("array", parser.currentName());
            assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken());
            List<String> values = new ArrayList<>();
            while(parser.nextToken().isValue()) {
                values.add(parser.text());
            }
            assertEquals(XContentParser.Token.END_ARRAY, parser.currentToken());
            CustomResponseSection2 responseSection2 = new CustomResponseSection2(values.toArray(new String[values.size()]));
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            return responseSection2;
        }
    }
}
