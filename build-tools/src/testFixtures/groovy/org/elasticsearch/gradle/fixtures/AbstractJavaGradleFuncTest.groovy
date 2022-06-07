/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.fixtures

class AbstractJavaGradleFuncTest extends AbstractGradleFuncTest {

    File testClazz(String testClassName) {
        testClazz(testClassName, {})
    }

    File testClazz(String testClassName, String parent) {
        testClazz(testClassName, parent, {})
    }

    File testClazz(String testClassName, Closure<String> content) {
        testClazz(testClassName, null, content)
    }

    File testClazz(String testClassName, parent, Closure<String> content) {
        def testClassFile = file("src/test/java/${testClassName.replace('.', '/')}.java")
        writeClazz(testClassName, parent, testClassFile, content)
    }

    File clazz(String className, parent = null, Closure<String> content = null) {
        def classFile = file("src/main/java/${className.replace('.', '/')}.java")
        writeClazz(className, parent, classFile, content)
    }

    static File writeClazz(String className, String parent, File classFile, Closure<String> content) {
        def packageName = className.substring(0, className.lastIndexOf('.'))
        def simpleClassName = className.substring(className.lastIndexOf('.') + 1)

        classFile << """
        package ${packageName};
        public class ${simpleClassName} ${parent == null ? "" : "extends $parent"} {
            ${content == null ? "" : content.call()}
        }
        """
        classFile
    }

}
