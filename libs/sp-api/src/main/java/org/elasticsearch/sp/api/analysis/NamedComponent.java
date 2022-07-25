/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.sp.api.analysis;

import org.elasticsearch.sp.api.analysis.annotations.NamedAnalysisComponent;

/**
 * A named analysis component. Analysis components with a name can be registered and fetch under a name given in
 * <code>@NamedAnalysisComponent</code>
 * @see NamedAnalysisComponent
 */
public sealed interface NamedComponent permits TokenFilterFactory,TokenizerFactory,CharFilterFactory,Analyzer {

    default String name() {
        NamedAnalysisComponent[] annotationsByType = this.getClass().getAnnotationsByType(NamedAnalysisComponent.class);
        if (annotationsByType.length == 1) {
            return annotationsByType[0].name();
        }
        return null;
    }

}
