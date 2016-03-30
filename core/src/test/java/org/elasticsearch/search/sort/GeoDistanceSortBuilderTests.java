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

package org.elasticsearch.search.sort;


import org.apache.lucene.search.SortField;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.test.geo.RandomGeoGenerator;

import java.io.IOException;
import java.util.Arrays;

public class GeoDistanceSortBuilderTests extends AbstractSortTestCase<GeoDistanceSortBuilder> {

    @Override
    protected GeoDistanceSortBuilder createTestItem() {
        return randomGeoDistanceSortBuilder();
    }

    public static GeoDistanceSortBuilder randomGeoDistanceSortBuilder() {
        String fieldName = randomAsciiOfLengthBetween(1, 10);
        GeoDistanceSortBuilder result = null;

        int id = randomIntBetween(0, 2);
        switch(id) {
            case 0:
                int count = randomIntBetween(1, 10);
                String[] geohashes = new String[count];
                for (int i = 0; i < count; i++) {
                    geohashes[i] = RandomGeoGenerator.randomPoint(getRandom()).geohash();
                }

                result = new GeoDistanceSortBuilder(fieldName, geohashes);
                break;
            case 1:
                GeoPoint pt = RandomGeoGenerator.randomPoint(getRandom());
                result = new GeoDistanceSortBuilder(fieldName, pt.getLat(), pt.getLon());
                break;
            case 2:
                result = new GeoDistanceSortBuilder(fieldName, points(new GeoPoint[0]));
                break;
            default:
                throw new IllegalStateException("one of three geo initialisation strategies must be used");

        }
        if (randomBoolean()) {
            result.geoDistance(geoDistance(result.geoDistance()));
        }
        if (randomBoolean()) {
            result.unit(unit(result.unit()));
        }
        if (randomBoolean()) {
            result.order(RandomSortDataGenerator.order(null));
        }
        if (randomBoolean()) {
            result.sortMode(mode(result.sortMode()));
        }
        if (randomBoolean()) {
            result.setNestedFilter(RandomSortDataGenerator.nestedFilter(result.getNestedFilter()));
        }
        if (randomBoolean()) {
            result.setNestedPath(RandomSortDataGenerator.randomAscii(result.getNestedPath()));
        }
        if (randomBoolean()) {
            result.coerce(! result.coerce());
        }
        if (randomBoolean()) {
            result.ignoreMalformed(! result.ignoreMalformed());
        }

        return result;
    }

    @Override
    protected MappedFieldType provideMappedFieldType(String name) {
        MappedFieldType clone = GeoPointFieldMapper.Defaults.FIELD_TYPE.clone();
        clone.setName(name);
        return clone;
    }

    private static SortMode mode(SortMode original) {
        SortMode result;
        do {
            result = randomFrom(SortMode.values());
        } while (result == SortMode.SUM || result == original);
        return result;
    }

    private static DistanceUnit unit(DistanceUnit original) {
        int id = -1;
        while (id == -1 || (original != null && original.ordinal() == id)) {
            id = randomIntBetween(0, DistanceUnit.values().length - 1);
        }
        return DistanceUnit.values()[id];
    }

    private static GeoPoint[] points(GeoPoint[] original) {
        GeoPoint[] result = null;
        while (result == null || Arrays.deepEquals(original, result)) {
            int count = randomIntBetween(1, 10);
            result = new GeoPoint[count];
            for (int i = 0; i < count; i++) {
                result[i] = RandomGeoGenerator.randomPoint(getRandom());
            }
        }
        return result;
    }

    private static GeoDistance geoDistance(GeoDistance original) {
        int id = -1;
        while (id == -1 || (original != null && original.ordinal() == id)) {
            id = randomIntBetween(0, GeoDistance.values().length - 1);
        }
        return GeoDistance.values()[id];
    }

    @Override
    protected GeoDistanceSortBuilder mutate(GeoDistanceSortBuilder original) throws IOException {
        GeoDistanceSortBuilder result = new GeoDistanceSortBuilder(original);
        int parameter = randomIntBetween(0, 9);
        switch (parameter) {
        case 0:
            while (Arrays.deepEquals(original.points(), result.points())) {
                GeoPoint pt = RandomGeoGenerator.randomPoint(getRandom());
                result.point(pt.getLat(), pt.getLon());
            }
            break;
        case 1:
            result.points(points(original.points()));
            break;
        case 2:
            result.geoDistance(geoDistance(original.geoDistance()));
            break;
        case 3:
            result.unit(unit(original.unit()));
            break;
        case 4:
            result.order(RandomSortDataGenerator.order(original.order()));
            break;
        case 5:
            result.sortMode(mode(original.sortMode()));
            break;
        case 6:
            result.setNestedFilter(RandomSortDataGenerator.nestedFilter(original.getNestedFilter()));
            break;
        case 7:
            result.setNestedPath(RandomSortDataGenerator.randomAscii(original.getNestedPath()));
            break;
        case 8:
            result.coerce(! original.coerce());
            break;
        case 9:
            // ignore malformed will only be set if coerce is set to true
            result.coerce(false);
            result.ignoreMalformed(! original.ignoreMalformed());
            break;
        }
        return result;
    }

    @Override
    protected void sortFieldAssertions(GeoDistanceSortBuilder builder, SortField sortField) throws IOException {
        assertEquals(SortField.Type.CUSTOM, sortField.getType());
        assertEquals(builder.order() == SortOrder.ASC ? false : true, sortField.getReverse());
        assertEquals(builder.fieldName(), sortField.getField());
    }

    public void testSortModeSumIsRejectedInSetter() {
        GeoDistanceSortBuilder builder = new GeoDistanceSortBuilder("testname", -1, -1);
        GeoPoint point = RandomGeoGenerator.randomPoint(getRandom());
        builder.point(point.getLat(), point.getLon());
        try {
            builder.sortMode(SortMode.SUM);
            fail("sort mode sum should not be supported");
          } catch (IllegalArgumentException e) {
              // all good
          }
    }

    public void testReverseOptionFailsWhenNonStringField() throws IOException {
        String json = "{\n" +
                "  \"testname\" : [ {\n" +
                "    \"lat\" : -6.046997540714173,\n" +
                "    \"lon\" : -51.94128329747579\n" +
                "  } ],\n" +
                "  \"reverse\" : true\n" +
                "}";
        XContentParser itemParser = XContentHelper.createParser(new BytesArray(json));
        itemParser.nextToken();

        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.reset(itemParser);

        try {
          GeoDistanceSortBuilder.fromXContent(context, "");
          fail("adding reverse sorting option should fail with an exception");
        } catch (ParsingException e) {
            assertEquals("Only geohashes of type string supported for field [reverse]", e.getMessage());
        }
    }

    public void testReverseOptionFailsWhenStringFieldButResetting() throws IOException {
        String json = "{\n" +
                "  \"testname\" : [ {\n" +
                "    \"lat\" : -6.046997540714173,\n" +
                "    \"lon\" : -51.94128329747579\n" +
                "  } ],\n" +
                "  \"reverse\" : \"true\"\n" +
                "}";
        XContentParser itemParser = XContentHelper.createParser(new BytesArray(json));
        itemParser.nextToken();

        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.reset(itemParser);

        try {
          GeoDistanceSortBuilder.fromXContent(context, "");
          fail("adding reverse sorting option should fail with an exception");
        } catch (ParsingException e) {
            assertEquals("Trying to reset fieldName to [reverse], already set to [testname].", e.getMessage());
        }
    }

    public void testReverseOptionFailsBuildWhenInvalidGeoHashString() throws IOException {
        String json = "{\n" +
                "  \"reverse\" : \"false\"\n" +
                "}";
        XContentParser itemParser = XContentHelper.createParser(new BytesArray(json));
        itemParser.nextToken();

        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.reset(itemParser);

        try {
          GeoDistanceSortBuilder item = GeoDistanceSortBuilder.fromXContent(context, "");
          item.ignoreMalformed(false);
          item.build(createMockShardContext());
          
          fail("adding reverse sorting option should fail with an exception");
        } catch (ElasticsearchParseException e) {
            assertEquals("illegal latitude value [269.384765625] for [GeoDistanceSort] for field [reverse].", e.getMessage());
        }
    }

    public void testSortModeSumIsRejectedInJSON() throws IOException {
        String json = "{\n" +
                "  \"testname\" : [ {\n" +
                "    \"lat\" : -6.046997540714173,\n" +
                "    \"lon\" : -51.94128329747579\n" +
                "  } ],\n" +
                "  \"unit\" : \"m\",\n" +
                "  \"distance_type\" : \"sloppy_arc\",\n" +
                "  \"mode\" : \"SUM\",\n" +
                "  \"coerce\" : false,\n" +
                "  \"ignore_malformed\" : false\n" +
                "}";
        XContentParser itemParser = XContentHelper.createParser(new BytesArray(json));
        itemParser.nextToken();

        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.reset(itemParser);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> GeoDistanceSortBuilder.fromXContent(context, ""));
        assertEquals("sort_mode [sum] isn't supported for sorting by geo distance", e.getMessage());
    }

    public void testGeoDistanceSortCanBeParsedFromGeoHash() throws IOException {
        String json = "{\n" +
                "    \"VDcvDuFjE\" : [ \"7umzzv8eychg\", \"dmdgmt5z13uw\", " +
                "    \"ezu09wxw6v4c\", \"kc7s3515p6k6\", \"jgeuvjwrmfzn\", \"kcpcfj7ruyf8\" ],\n" +
                "    \"unit\" : \"m\",\n" +
                "    \"distance_type\" : \"sloppy_arc\",\n" +
                "    \"mode\" : \"MAX\",\n" +
                "    \"nested_filter\" : {\n" +
                "      \"ids\" : {\n" +
                "        \"type\" : [ ],\n" +
                "        \"values\" : [ ],\n" +
                "        \"boost\" : 5.711116\n" +
                "      }\n" +
                "    },\n" +
                "    \"coerce\" : false,\n" +
                "    \"ignore_malformed\" : true\n" +
                "  }";
        XContentParser itemParser = XContentHelper.createParser(new BytesArray(json));
        itemParser.nextToken();

        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.reset(itemParser);

        GeoDistanceSortBuilder result = GeoDistanceSortBuilder.fromXContent(context, json);
        assertEquals("[-19.700583312660456, -2.8225036337971687, "
                + "31.537466906011105, -74.63590376079082, "
                + "43.71844606474042, -5.548660643398762, "
                + "-37.20467280596495, 38.71751043945551, "
                + "-69.44606635719538, 84.25200328230858, "
                + "-39.03717711567879, 44.74099852144718]", Arrays.toString(result.points()));
    }

    @Override
    protected GeoDistanceSortBuilder fromXContent(QueryParseContext context, String fieldName) throws IOException {
        return GeoDistanceSortBuilder.fromXContent(context, fieldName);
    }
}
