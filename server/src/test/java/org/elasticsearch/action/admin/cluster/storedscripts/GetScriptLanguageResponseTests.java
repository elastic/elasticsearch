/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.script.ScriptLanguagesInfo;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class GetScriptLanguageResponseTests extends AbstractXContentSerializingTestCase<GetScriptLanguageResponse> {

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ScriptLanguagesInfo, Void> PARSER = new ConstructingObjectParser<>(
        "script_languages_info",
        true,
        (a) -> new ScriptLanguagesInfo(
            new HashSet<>((List<String>) a[0]),
            ((List<Tuple<String, Set<String>>>) a[1]).stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2))
        )
    );

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Tuple<String, Set<String>>, Void> LANGUAGE_CONTEXT_PARSER =
        new ConstructingObjectParser<>("language_contexts", true, (m, name) -> new Tuple<>((String) m[0], Set.copyOf((List<String>) m[1])));

    static {
        PARSER.declareStringArray(constructorArg(), ScriptLanguagesInfo.TYPES_ALLOWED);
        PARSER.declareObjectArray(constructorArg(), LANGUAGE_CONTEXT_PARSER, ScriptLanguagesInfo.LANGUAGE_CONTEXTS);
        LANGUAGE_CONTEXT_PARSER.declareString(constructorArg(), ScriptLanguagesInfo.LANGUAGE);
        LANGUAGE_CONTEXT_PARSER.declareStringArray(constructorArg(), ScriptLanguagesInfo.CONTEXTS);
    }

    private static int MAX_VALUES = 4;
    private static final int MIN_LENGTH = 1;
    private static final int MAX_LENGTH = 16;

    @Override
    protected GetScriptLanguageResponse createTestInstance() {
        if (randomBoolean()) {
            return new GetScriptLanguageResponse(new ScriptLanguagesInfo(Collections.emptySet(), Collections.emptyMap()));
        }
        return new GetScriptLanguageResponse(randomInstance());
    }

    @Override
    protected GetScriptLanguageResponse doParseInstance(XContentParser parser) throws IOException {
        return new GetScriptLanguageResponse(PARSER.parse(parser, null));
    }

    @Override
    protected Writeable.Reader<GetScriptLanguageResponse> instanceReader() {
        return GetScriptLanguageResponse::new;
    }

    @Override
    protected GetScriptLanguageResponse mutateInstance(GetScriptLanguageResponse instance) {
        switch (randomInt(2)) {
            case 0:
                // mutate typesAllowed
                return new GetScriptLanguageResponse(
                    new ScriptLanguagesInfo(mutateStringSet(instance.info.typesAllowed), instance.info.languageContexts)
                );
            case 1:
                // Add language
                String language = randomValueOtherThanMany(
                    instance.info.languageContexts::containsKey,
                    () -> randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH)
                );
                Map<String, Set<String>> languageContexts = new HashMap<>();
                instance.info.languageContexts.forEach(languageContexts::put);
                languageContexts.put(language, randomStringSet(randomIntBetween(1, MAX_VALUES)));
                return new GetScriptLanguageResponse(new ScriptLanguagesInfo(instance.info.typesAllowed, languageContexts));
            default:
                // Mutate languageContexts
                Map<String, Set<String>> lc = new HashMap<>();
                if (instance.info.languageContexts.size() == 0) {
                    lc.put(randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH), randomStringSet(randomIntBetween(1, MAX_VALUES)));
                } else {
                    int toModify = randomInt(instance.info.languageContexts.size() - 1);
                    List<String> keys = new ArrayList<>(instance.info.languageContexts.keySet());
                    for (int i = 0; i < keys.size(); i++) {
                        String key = keys.get(i);
                        Set<String> value = instance.info.languageContexts.get(keys.get(i));
                        if (i == toModify) {
                            value = mutateStringSet(instance.info.languageContexts.get(keys.get(i)));
                        }
                        lc.put(key, value);
                    }
                }
                return new GetScriptLanguageResponse(new ScriptLanguagesInfo(instance.info.typesAllowed, lc));
        }
    }

    private static ScriptLanguagesInfo randomInstance() {
        Map<String, Set<String>> contexts = new HashMap<>();
        for (String context : randomStringSet(randomIntBetween(1, MAX_VALUES))) {
            contexts.put(context, randomStringSet(randomIntBetween(1, MAX_VALUES)));
        }
        return new ScriptLanguagesInfo(randomStringSet(randomInt(MAX_VALUES)), contexts);
    }

    private static Set<String> randomStringSet(int numInstances) {
        Set<String> rand = Sets.newHashSetWithExpectedSize(numInstances);
        for (int i = 0; i < numInstances; i++) {
            rand.add(randomValueOtherThanMany(rand::contains, () -> randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH)));
        }
        return rand;
    }

    private static Set<String> mutateStringSet(Set<String> strings) {
        if (strings.isEmpty()) {
            return Set.of(randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH));
        }

        if (randomBoolean()) {
            Set<String> updated = new HashSet<>(strings);
            updated.add(randomValueOtherThanMany(updated::contains, () -> randomAlphaOfLengthBetween(MIN_LENGTH, MAX_LENGTH)));
            return updated;
        } else {
            List<String> sorted = strings.stream().sorted().toList();
            int toRemove = randomInt(sorted.size() - 1);
            Set<String> updated = new HashSet<>();
            for (int i = 0; i < sorted.size(); i++) {
                if (i != toRemove) {
                    updated.add(sorted.get(i));
                }
            }
            return updated;
        }
    }
}
