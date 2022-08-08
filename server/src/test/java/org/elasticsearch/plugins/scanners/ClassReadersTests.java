/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import junit.framework.TestCase;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClassReadersTests extends ESTestCase {

    public void testOfModulePath() throws IOException {
        String path = ClassReadersTests.class.getClassLoader().getResource("api-jars/api.jar").getPath();
        String dirWithJar = path.substring(0, path.length() - "api.jar".length());

        Stream<ClassReader> classReaderStream = ClassReaders.ofDirWithJars(dirWithJar);

        List<String> collect = classReaderStream.map(cr -> cr.getClassName()).collect(Collectors.toList());
        assertThat(collect,
            Matchers.contains(
                "org/elasticsearch/component/Component",
                "org/elasticsearch/component/ExtensibleComponent",
                "org/elasticsearch/component/NamedComponent",
                "org/elasticsearch/component/Nameable",
                "org/elasticsearch/test/Super",
                "org/elasticsearch/test/Super2",
                "org/elasticsearch/test/Mid",
                "org/elasticsearch/analysis/TokenizerFactory",
                "org/elasticsearch/analysis/AnalysisBase",
                "org/elasticsearch/analysis/Tokenizer",
                 "org/elasticsearch/analysis/TokenFilterFactory"
            ));
    }
}
