/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test.rest.transform.warnings;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.gradle.test.rest.transform.RestTestTransformByParentObject;
import org.elasticsearch.gradle.test.rest.transform.feature.FeatureInjector;

import java.util.List;

//TODO
public class InjectWarnings extends FeatureInjector implements RestTestTransformByParentObject {

    private static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);

    private final List<String> warnings;

    /**
     * @param warnings The allowed warnings to inject
     */
    public InjectWarnings(List<String> warnings) {
        this.warnings = warnings;
    }

    @Override
    public void transformTest(ObjectNode doNodeParent) {
        ObjectNode doNodeValue = (ObjectNode) doNodeParent.get(getKeyToFind());
        ArrayNode arrayWarnings = (ArrayNode) doNodeValue.get("warnings");
        if (arrayWarnings == null) {
            arrayWarnings = new ArrayNode(jsonNodeFactory);
            doNodeValue.set("warnings", arrayWarnings);
        }
        warnings.forEach(arrayWarnings::add);
    }

    @Override
    public String getKeyToFind() {
        return "do";
    }

    @Override
    public String getSkipFeatureName() {
        return "warnings";
    }
}
