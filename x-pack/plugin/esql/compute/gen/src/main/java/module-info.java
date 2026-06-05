/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.elasticsearch.compute.gen.AggregatorProcessor;
import org.elasticsearch.compute.gen.ConsumeProcessor;
import org.elasticsearch.compute.gen.EvaluatorProcessor;
import org.elasticsearch.compute.gen.FunctionInfoProcessor;
import org.elasticsearch.compute.gen.MarkerAnnotationProcessor;

module org.elasticsearch.compute.gen {
    requires com.squareup.javapoet;
    requires org.elasticsearch.compute.ann;
    requires java.compiler;
    requires org.elasticsearch.base;
    requires org.elasticsearch.xcontent;

    exports org.elasticsearch.compute.gen;
    exports org.elasticsearch.compute.gen.argument;

    provides javax.annotation.processing.Processor
        with
            AggregatorProcessor,
            ConsumeProcessor,
            EvaluatorProcessor,
            FunctionInfoProcessor,
            MarkerAnnotationProcessor;
}
