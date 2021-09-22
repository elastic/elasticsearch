/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.rewrite.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.openrewrite.Recipe;
import org.openrewrite.internal.lang.NonNull;

public class ChangeMethodOwnerRecipe extends Recipe {
    @Override
    public String getDisplayName() {
        return "Change Method Owner Recipe chain";
    }

    @JsonCreator
    public ChangeMethodOwnerRecipe(@NonNull @JsonProperty("originFullQualifiedClassname") String originFullQualifiedClassname,
                                   @NonNull @JsonProperty("methodName") String methodName,
                                   @NonNull @JsonProperty("targetFullQualifiedClassname") String targetFullQualifiedClassname) {
        doNext(new FullQualifiedChangeMethodOwnerRecipe(originFullQualifiedClassname, methodName, targetFullQualifiedClassname));
        doNext(new FixFullQualifiedReferenceRecipe(targetFullQualifiedClassname, true));
    }

}

