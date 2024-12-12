/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
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

    private record ConflictCheck(XContentBuilder init, XContentBuilder update, Consumer<DocumentMapper> check) {}

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
            }), d -> {}));
        }

        /**
         * Register a check that a parameter update will cause a conflict
         *
         * @param param  the parameter name, expected to appear in the error message
         * @param init   the initial mapping
         * @param update the updated mapping
         */
        public void registerConflictCheck(String param, XContentBuilder init, XContentBuilder update, Consumer<DocumentMapper> check) {
            conflictChecks.put(param, new ConflictCheck(init, update, check));
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
            checker.conflictChecks.get(param).check.accept(mapperService.documentMapper());
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
            () -> mapperService.parseMapping("_doc", MergeReason.MAPPING_UPDATE, new CompressedXContent(mappingAsString))
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
            () -> mapperService.parseMapping("_doc", MergeReason.MAPPING_UPDATE, new CompressedXContent(mappingAsString))
        );
        assertEquals("Failed to parse mapping: " + fieldName() + " is not configurable", exception.getMessage());
    }
}
