/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;

public abstract class MetadataMapperTestCase extends MapperServiceTestCase {

    protected abstract String fieldName();

    protected abstract boolean isConfigurable();

    protected boolean isSupportedOn(IndexVersion version) {
        return true;
    }

    protected abstract void registerParameters(ParameterChecker checker) throws IOException;

    private record ConflictCheck(XContentBuilder init, XContentBuilder update) {}

    private record UpdateCheck(XContentBuilder init, XContentBuilder update, Consumer<DocumentMapper> check) {}

    public class ParameterChecker {

        Map<String, ConflictCheck> conflictChecks = new HashMap<>();
        List<UpdateCheck> updateChecks = new ArrayList<>();

        /**
         * Register a check that a parameter update will cause a conflict, using the minimal mapping as a base
         *
         * @param param  the parameter name, expected to appear in the error message
         * @param update a field builder applied on top of the minimal mapping
         */
        public void registerConflictCheck(String param, CheckedConsumer<XContentBuilder, IOException> update) throws IOException {
            conflictChecks.put(param, new ConflictCheck(topMapping(b -> b.startObject(fieldName()).endObject()), topMapping(b -> {
                b.startObject(fieldName());
                update.accept(b);
                b.endObject();
            })));
        }

        /**
         * Register a check that a parameter update will cause a conflict
         *
         * @param param  the parameter name, expected to appear in the error message
         * @param init   the initial mapping
         * @param update the updated mapping
         */
        public void registerConflictCheck(String param, XContentBuilder init, XContentBuilder update) {
            conflictChecks.put(param, new ConflictCheck(init, update));
        }

        public void registerUpdateCheck(XContentBuilder init, XContentBuilder update, Consumer<DocumentMapper> check) {
            updateChecks.add(new UpdateCheck(init, update, check));
        }
    }

    public final void testUpdates() throws IOException {
        assumeTrue("Metadata field " + fieldName() + " isn't configurable", isConfigurable());
        ParameterChecker checker = new ParameterChecker();
        registerParameters(checker);
        for (String param : checker.conflictChecks.keySet()) {
            MapperService mapperService = createMapperService(checker.conflictChecks.get(param).init);
            // merging the same change is fine
            merge(mapperService, checker.conflictChecks.get(param).init);
            // merging the conflicting update should throw an exception
            Exception e = expectThrows(
                IllegalArgumentException.class,
                "No conflict when updating parameter [" + param + "]",
                () -> merge(mapperService, checker.conflictChecks.get(param).update)
            );
            assertThat(
                e.getMessage(),
                anyOf(containsString("Cannot update parameter [" + param + "]"), containsString("different [" + param + "]"))
            );
        }
        for (UpdateCheck updateCheck : checker.updateChecks) {
            MapperService mapperService = createMapperService(updateCheck.init);
            // merging is fine
            merge(mapperService, updateCheck.update);
            // run the update assertion
            updateCheck.check.accept(mapperService.documentMapper());
        }
    }

    public final void testUnsupportedParametersAreRejected() throws IOException {
        assumeTrue("Metadata field " + fieldName() + " isn't configurable", isConfigurable());
        IndexVersion version = IndexVersionUtils.randomCompatibleVersion(random());
        assumeTrue("Metadata field " + fieldName() + " is not supported on version " + version, isSupportedOn(version));
        MapperService mapperService = createMapperService(version, mapping(xContentBuilder -> {}));
        String mappingAsString = "{\n"
            + "    \"_doc\" : {\n"
            + "      \""
            + fieldName()
            + "\" : {\n"
            + "        \"anything\" : \"anything\"\n"
            + "      }\n"
            + "    }\n"
            + "}";
        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> mapperService.parseMapping("_doc", new CompressedXContent(mappingAsString))
        );
        assertEquals(
            "Failed to parse mapping: unknown parameter [anything] on metadata field [" + fieldName() + "]",
            exception.getMessage()
        );
    }

    public final void testFixedMetaFieldsAreNotConfigurable() throws IOException {
        assumeFalse("Metadata field " + fieldName() + " is configurable", isConfigurable());
        IndexVersion version = IndexVersionUtils.randomCompatibleVersion(random());
        assumeTrue("Metadata field " + fieldName() + " is not supported on version " + version, isSupportedOn(version));
        MapperService mapperService = createMapperService(version, mapping(xContentBuilder -> {}));
        String mappingAsString = "{\n" + "    \"_doc\" : {\n" + "      \"" + fieldName() + "\" : {\n" + "      }\n" + "    }\n" + "}";
        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> mapperService.parseMapping("_doc", new CompressedXContent(mappingAsString))
        );
        assertEquals("Failed to parse mapping: " + fieldName() + " is not configurable", exception.getMessage());
    }

    public void testTypeAndFriendsAreAcceptedBefore_8_6_0() throws IOException {
        assumeTrue("Metadata field " + fieldName() + " isn't configurable", isConfigurable());
        IndexVersion previousVersion = IndexVersionUtils.getPreviousVersion(IndexVersion.V_8_6_0);
        IndexVersion version = IndexVersionUtils.randomVersionBetween(random(), IndexVersion.V_7_0_0, previousVersion);
        assumeTrue("Metadata field " + fieldName() + " is not supported on version " + version, isSupportedOn(version));
        MapperService mapperService = createMapperService(version, mapping(b -> {}));
        // these parameters were previously silently ignored, they will still be ignored in existing indices
        String[] unsupportedParameters = new String[] { "fields", "copy_to", "boost", "type" };
        for (String param : unsupportedParameters) {
            String mappingAsString = "{\n"
                + "    \"_doc\" : {\n"
                + "      \""
                + fieldName()
                + "\" : {\n"
                + "        \""
                + param
                + "\" : \"any\"\n"
                + "      }\n"
                + "    }\n"
                + "}";
            assertNotNull(mapperService.parseMapping("_doc", new CompressedXContent(mappingAsString)));
        }
    }

    public void testTypeAndFriendsAreDeprecatedFrom_8_6_0() throws IOException {
        assumeTrue("Metadata field " + fieldName() + " isn't configurable", isConfigurable());
        IndexVersion version = IndexVersionUtils.randomVersionBetween(random(), IndexVersion.V_8_6_0, IndexVersion.current());
        assumeTrue("Metadata field " + fieldName() + " is not supported on version " + version, isSupportedOn(version));
        MapperService mapperService = createMapperService(version, mapping(b -> {}));
        // these parameters were previously silently ignored, they are now deprecated in new indices
        String[] unsupportedParameters = new String[] { "fields", "copy_to", "boost", "type" };
        for (String param : unsupportedParameters) {
            String mappingAsString = "{\n"
                + "    \"_doc\" : {\n"
                + "      \""
                + fieldName()
                + "\" : {\n"
                + "        \""
                + param
                + "\" : \"any\"\n"
                + "      }\n"
                + "    }\n"
                + "}";
            assertNotNull(mapperService.parseMapping("_doc", new CompressedXContent(mappingAsString)));
            assertWarnings("Parameter [" + param + "] has no effect on metadata field [" + fieldName() + "] and will be removed in future");
        }
    }
}
