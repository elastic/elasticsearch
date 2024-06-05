/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.painlesswhitelist;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistInstanceBinding;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.painless.spi.annotation.WhitelistAnnotationParser;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.script.ScriptContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** An extension of painless which adds a whitelist. */
public class ExampleWhitelistExtension implements PainlessExtension {

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        Map<String, WhitelistAnnotationParser> parsers = new HashMap<>(WhitelistAnnotationParser.BASE_ANNOTATION_PARSERS);
        parsers.put(ExamplePainlessAnnotation.NAME, ExampleWhitelistAnnotationParser.INSTANCE);
        Whitelist classWhitelist =
                WhitelistLoader.loadFromResourceFiles(ExampleWhitelistExtension.class, parsers, "example_whitelist.txt");

        ExampleWhitelistedInstance ewi = new ExampleWhitelistedInstance(1);
        WhitelistInstanceBinding addValue = new WhitelistInstanceBinding("example addValue", ewi,
            "addValue", "int", Collections.singletonList("int"), Collections.emptyList());
        WhitelistInstanceBinding getValue = new WhitelistInstanceBinding("example getValue", ewi,
            "getValue", "int", Collections.emptyList(), Collections.emptyList());
        Whitelist instanceWhitelist = new Whitelist(ewi.getClass().getClassLoader(), Collections.emptyList(),
            Collections.emptyList(), Collections.emptyList(), Arrays.asList(addValue, getValue));

        return Collections.singletonMap(FieldScript.CONTEXT, Arrays.asList(classWhitelist, instanceWhitelist));
    }
}
