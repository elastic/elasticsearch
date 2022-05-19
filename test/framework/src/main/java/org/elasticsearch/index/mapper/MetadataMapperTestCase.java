/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.core.CheckedConsumer;
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
}
