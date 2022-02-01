/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.modules

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest

class AbstractJavaModulesPluginFuncTest extends AbstractGradleFuncTest {

    void writeModuleInfo(File root = testProjectDir.root, String name = root.name) {
        file(root,'src/main/java/module-info.java') << """
module org.example.${name} {
    exports org.example.${name}.api;
    uses org.example.${name}.api.Component;
}
"""

    }

    void writeProducingJavaSource(File root = testProjectDir.root, String cName = root.name) {
        println "cName = $cName"
        file(root, "src/main/java/org/example/${cName}/impl/SomethingInternal.java") << """package org.example.${cName}.impl;

public class SomethingInternal {

    public void doSomething() {
        System.out.println("Something internal");
    }
}
"""

        file(root,"src/main/java/org/example/${ cName}/api/Component.java") << """package org.example.${cName}.api;

public class Component {

    public Component() {
    }
    
    public void doSomething() {
        new org.example.${cName}.impl.SomethingInternal().doSomething();
    }

}
"""
    }

    void writeConsumingInternalJavaSource(File root = testProjectDir.root, String providingModuleName = 'producing') {
        def cName = root.name
        def clazzName = "Consuming${providingModuleName.capitalize()}Internal"

        file(root,"src/main/java/org/${cName}/${clazzName}.java") << """package org.${cName};

import org.example.${providingModuleName}.api.Component;
import org.example.${providingModuleName}.impl.SomethingInternal;

public class $clazzName {
    Component c = new Component();
    
    public void run() {
       SomethingInternal i = new SomethingInternal();
       i.doSomething();
    }

}
"""
    }

    void writeConsumingJavaSource(File root = testProjectDir.root, String providingModuleName = 'producing') {
        String name = root.name;
        def clazzName = "Consuming${providingModuleName.capitalize()}"
        file(root, "src/main/java/org/${name}/" + clazzName + ".java") << """package org.${name};

import org.example.${providingModuleName}.api.Component;

public class $clazzName {
    Component c = new Component();
    
    public void run() {
       c.doSomething();
    }

}
"""
    }

}
