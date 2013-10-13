/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.mapper.date;

import org.apache.lucene.analysis.NumericTokenStream.NumericTermAttribute;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeFilter;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class SimpleDateMappingTests extends ElasticsearchTestCase {

    @Test
    public void testAutomaticDateParser() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = mapper(mapping);

        defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("date_field1", "2011/01/22")
                .field("date_field2", "2011/01/22 00:00:00")
                .field("wrong_date1", "-4")
                .field("wrong_date2", "2012/2")
                .field("wrong_date3", "2012/test")
                .endObject()
                .bytes());

        FieldMapper<?> fieldMapper = defaultMapper.mappers().smartNameFieldMapper("date_field1");
        assertThat(fieldMapper, instanceOf(DateFieldMapper.class));
        fieldMapper = defaultMapper.mappers().smartNameFieldMapper("date_field2");
        assertThat(fieldMapper, instanceOf(DateFieldMapper.class));

        fieldMapper = defaultMapper.mappers().smartNameFieldMapper("wrong_date1");
        assertThat(fieldMapper, instanceOf(StringFieldMapper.class));
        fieldMapper = defaultMapper.mappers().smartNameFieldMapper("wrong_date2");
        assertThat(fieldMapper, instanceOf(StringFieldMapper.class));
        fieldMapper = defaultMapper.mappers().smartNameFieldMapper("wrong_date3");
        assertThat(fieldMapper, instanceOf(StringFieldMapper.class));
    }
    
    @Test
    public void testParseLocal() {
        assertThat(Locale.GERMAN, equalTo(DateFieldMapper.parseLocale("de")));
        assertThat(Locale.GERMANY, equalTo(DateFieldMapper.parseLocale("de_DE")));
        assertThat(new Locale("de","DE","DE"), equalTo(DateFieldMapper.parseLocale("de_DE_DE")));
        
        try {
            DateFieldMapper.parseLocale("de_DE_DE_DE");
            assert false;
        } catch(ElasticSearchIllegalArgumentException ex) {
            // expected
        }
        assertThat(Locale.ROOT,  equalTo(DateFieldMapper.parseLocale("")));
        assertThat(Locale.ROOT,  equalTo(DateFieldMapper.parseLocale("ROOT")));
    }
    
    @Test
    public void testLocale() throws IOException {
        String mapping = XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject("type")
                            .startObject("properties")
                                .startObject("date_field_default")
                                    .field("type", "date")
                                    .field("format", "E, d MMM yyyy HH:mm:ss Z")
                                .endObject()
                                .startObject("date_field_en")
                                    .field("type", "date")
                                    .field("format", "E, d MMM yyyy HH:mm:ss Z")
                                    .field("locale", "EN")
                                .endObject()
                                 .startObject("date_field_de")
                                    .field("type", "date")
                                    .field("format", "E, d MMM yyyy HH:mm:ss Z")
                                    .field("locale", "DE_de")
                                .endObject()
                            .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = mapper(mapping);
        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                  .field("date_field_en", "Wed, 06 Dec 2000 02:55:00 -0800")
                  .field("date_field_de", "Mi, 06 Dez 2000 02:55:00 -0800")
                  .field("date_field_default", "Wed, 06 Dec 2000 02:55:00 -0800") // check default - no exception is a successs!
                .endObject()
                .bytes());
        assertNumericTokensEqual(doc, defaultMapper, "date_field_en", "date_field_de");
        assertNumericTokensEqual(doc, defaultMapper, "date_field_en", "date_field_default");
    }
    
    private DocumentMapper mapper(String mapping) throws IOException {
        // we serialize and deserialize the mapping to make sure serialization works just fine
        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        defaultMapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String rebuildMapping = builder.string();
        return MapperTestUtils.newParser().parse(rebuildMapping);
    }
    
    private void assertNumericTokensEqual(ParsedDocument doc, DocumentMapper defaultMapper, String fieldA, String fieldB) throws IOException {
        assertThat(doc.rootDoc().getField(fieldA).tokenStream(defaultMapper.indexAnalyzer()), notNullValue());
        assertThat(doc.rootDoc().getField(fieldB).tokenStream(defaultMapper.indexAnalyzer()), notNullValue());
        
        TokenStream tokenStream = doc.rootDoc().getField(fieldA).tokenStream(defaultMapper.indexAnalyzer());
        tokenStream.reset();
        NumericTermAttribute nta = tokenStream.addAttribute(NumericTermAttribute.class);
        List<Long> values = new ArrayList<Long>();
        while(tokenStream.incrementToken()) {
            values.add(nta.getRawValue());
        }
        
        tokenStream = doc.rootDoc().getField(fieldB).tokenStream(defaultMapper.indexAnalyzer());
        tokenStream.reset();
        nta = tokenStream.addAttribute(NumericTermAttribute.class);
        int pos = 0;
        while(tokenStream.incrementToken()) {
            assertThat(values.get(pos++), equalTo(nta.getRawValue()));
        }
        assertThat(pos, equalTo(values.size()));
    }

    @Test
    public void testTimestampAsDate() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("date_field").field("type", "date").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = mapper(mapping);

        long value = System.currentTimeMillis();
        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("date_field", value)
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("date_field").tokenStream(defaultMapper.indexAnalyzer()), notNullValue());
    }

    @Test
    public void testDateDetection() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .field("date_detection", false)
                .startObject("properties").startObject("date_field").field("type", "date").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = mapper(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("date_field", "2010-01-01")
                .field("date_field_x", "2010-01-01")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("date_field"), nullValue());
        assertThat(doc.rootDoc().get("date_field_x"), equalTo("2010-01-01"));
    }

    @Test
    public void testHourFormat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .field("date_detection", false)
                .startObject("properties").startObject("date_field").field("type", "date").field("format", "HH:mm:ss").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = mapper(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("date_field", "10:00:00")
                .endObject()
                .bytes());
        assertThat(((LongFieldMapper.CustomLongNumericField) doc.rootDoc().getField("date_field")).numericAsString(), equalTo(Long.toString(new DateTime(TimeValue.timeValueHours(10).millis(), DateTimeZone.UTC).getMillis())));

        Filter filter = defaultMapper.mappers().smartNameFieldMapper("date_field").rangeFilter("10:00:00", "11:00:00", true, true, null);
        assertThat(filter, instanceOf(NumericRangeFilter.class));
        NumericRangeFilter<Long> rangeFilter = (NumericRangeFilter<Long>) filter;
        assertThat(rangeFilter.getMax(), equalTo(new DateTime(TimeValue.timeValueHours(11).millis() + 999).getMillis())); // +999 to include the 00-01 minute
        assertThat(rangeFilter.getMin(), equalTo(new DateTime(TimeValue.timeValueHours(10).millis()).getMillis()));
    }

    @Test
    public void testIgnoreMalformedOption() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "date").field("ignore_malformed", true).endObject()
                .startObject("field2").field("type", "date").field("ignore_malformed", false).endObject()
                .startObject("field3").field("type", "date").endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = mapper(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "a")
                .field("field2", "2010-01-01")
                .endObject()
                .bytes());
        assertThat(doc.rootDoc().getField("field1"), nullValue());
        assertThat(doc.rootDoc().getField("field2"), notNullValue());

        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field2", "a")
                    .endObject()
                    .bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(MapperParsingException.class));
        }

        // Verify that the default is false
        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field3", "a")
                    .endObject()
                    .bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(MapperParsingException.class));
        }

        // Unless the global ignore_malformed option is set to true
        Settings indexSettings = settingsBuilder().put("index.mapping.ignore_malformed", true).build();
        defaultMapper = MapperTestUtils.newParser(indexSettings).parse(mapping);
        doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field3", "a")
                .endObject()
                .bytes());
        assertThat(doc.rootDoc().getField("field3"), nullValue());

        // This should still throw an exception, since field2 is specifically set to ignore_malformed=false
        try {
            defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                    .startObject()
                    .field("field2", "a")
                    .endObject()
                    .bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(MapperParsingException.class));
        }
    }
}