/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.entitlement.tools.AccessibleJdkMethods.AccessibleMethod;
import org.elasticsearch.entitlement.tools.AccessibleJdkMethods.ModuleClass;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.hasItem;

public class AccessibleJdkMethodsTests extends ESTestCase {

    public void testKnownJdkMethods() throws IOException {
        Map<ModuleClass, List<AccessibleMethod>> methodsByClass = AccessibleJdkMethods.loadAccessibleMethods(Utils.DEFAULT_MODULE_PREDICATE)
            .collect(Collectors.groupingBy(Tuple::v1, Collectors.mapping(Tuple::v2, Collectors.toList())));

        assertThat(
            "System methods should be loaded",
            methodsByClass.get(new ModuleClass("java.base", "java/lang/System")).stream().map(AccessibleMethod::descriptor).toList(),
            hasItem(new AccessibleMethod.Descriptor("exit", "(I)V", true, true))
        );

        assertThat(
            "Files methods should be loaded",
            methodsByClass.get(new ModuleClass("java.base", "java/nio/file/Files")).stream().map(AccessibleMethod::descriptor).toList(),
            hasItem(new AccessibleMethod.Descriptor("readAllBytes", "(Ljava/nio/file/Path;)[B", true, true))
        );

        assertTrue(
            "Object.getClass() final method should be loaded",
            methodsByClass.get(new ModuleClass("java.base", "java/lang/Object"))
                .stream()
                .filter(m -> "getClass".equals(m.descriptor().method()))
                .anyMatch(AccessibleMethod::isFinal)
        );

        assertTrue(
            "Some deprecated methods should be loaded",
            methodsByClass.values().stream().flatMap(List::stream).anyMatch(AccessibleMethod::isDeprecated)
        );
    }

    public void testExcludesToStringHashCodeEqualsClose() throws IOException {
        Set<AccessibleMethod.Descriptor> excludedDescriptors = Set.of(
            new AccessibleMethod.Descriptor("toString", "()Ljava/lang/String;", true, false),
            new AccessibleMethod.Descriptor("hashCode", "()I", true, false),
            new AccessibleMethod.Descriptor("equals", "(Ljava/lang/Object;)Z", true, false),
            new AccessibleMethod.Descriptor("close", "()V", true, false)
        );

        Set<AccessibleMethod.Descriptor> allDescriptors = AccessibleJdkMethods.loadAccessibleMethods(Utils.DEFAULT_MODULE_PREDICATE)
            .map(t -> t.v2().descriptor())
            .collect(Collectors.toSet());

        for (AccessibleMethod.Descriptor excluded : excludedDescriptors) {
            assertFalse("Should not contain " + excluded.method(), allDescriptors.contains(excluded));
        }
    }

    public void testExcludesComSunInternal() throws IOException {
        for (ModuleClass tuple : AccessibleJdkMethods.loadAccessibleMethods(Utils.DEFAULT_MODULE_PREDICATE).map(Tuple::v1).toList()) {
            String className = tuple.clazz();
            assertFalse(
                "com/sun/*/internal/* classes should be skipped: " + className,
                className.startsWith("com/sun/") && className.contains("/internal/")
            );
        }
    }

    public void testModuleFiltering() throws IOException {
        for (ModuleClass tuple : AccessibleJdkMethods.loadAccessibleMethods("java.sql"::equals).map(Tuple::v1).toList()) {
            assertEquals("Only the requested module should be included", "java.sql", tuple.module());
        }
    }

    public void testInheritedMethods() throws IOException {
        var readDescriptor = new AccessibleMethod.Descriptor("read", "([BII)I", true, false);
        for (String className : List.of("java/io/InputStream", "java/io/FilterInputStream", "java/io/BufferedInputStream")) {
            assertTrue(
                className + " should have read(byte[],int,int)",
                AccessibleJdkMethods.loadAccessibleMethods(Utils.DEFAULT_MODULE_PREDICATE)
                    .filter(t -> t.v1().clazz().equals(className))
                    .map(t -> t.v2().descriptor())
                    .anyMatch(readDescriptor::equals)
            );
        }
    }

    public void testConstructorsNotInherited() throws IOException {
        Map<ModuleClass, List<AccessibleMethod>> methodsByClass = AccessibleJdkMethods.loadAccessibleMethods(Utils.DEFAULT_MODULE_PREDICATE)
            .collect(Collectors.groupingBy(Tuple::v1, Collectors.mapping(Tuple::v2, Collectors.toList())));

        Set<String> parentCtors = methodsByClass.get(new ModuleClass("java.base", "java/io/InputStream"))
            .stream()
            .filter(m -> "<init>".equals(m.descriptor().method()))
            .map(m -> m.descriptor().descriptor())
            .collect(Collectors.toSet());
        Set<String> childCtors = methodsByClass.get(new ModuleClass("java.base", "java/io/BufferedInputStream"))
            .stream()
            .filter(m -> "<init>".equals(m.descriptor().method()))
            .map(m -> m.descriptor().descriptor())
            .collect(Collectors.toSet());

        assertTrue("Parent and child should have different constructors (no inheritance)", Collections.disjoint(parentCtors, childCtors));
    }

    public void testFinalClassExcludesProtectedMethods() throws IOException {
        Map<ModuleClass, List<AccessibleMethod>> methodsByClass = AccessibleJdkMethods.loadAccessibleMethods(Utils.DEFAULT_MODULE_PREDICATE)
            .collect(Collectors.groupingBy(Tuple::v1, Collectors.mapping(Tuple::v2, Collectors.toList())));

        var stringMethods = methodsByClass.get(new ModuleClass("java.base", "java/lang/String"));
        assertFalse(
            "Final class String should have no protected methods",
            stringMethods.stream().anyMatch(m -> m.descriptor().isPublic() == false)
        );
    }

    public void testNonFinalClassIncludesProtectedMethods() throws IOException {
        Map<ModuleClass, List<AccessibleMethod>> methodsByClass = AccessibleJdkMethods.loadAccessibleMethods(Utils.DEFAULT_MODULE_PREDICATE)
            .collect(Collectors.groupingBy(Tuple::v1, Collectors.mapping(Tuple::v2, Collectors.toList())));

        assertTrue(
            "Non-final class Object should include protected clone() method",
            methodsByClass.get(new ModuleClass("java.base", "java/lang/Object"))
                .stream()
                .filter(m -> "clone".equals(m.descriptor().method()))
                .anyMatch(m -> m.descriptor().isPublic() == false)
        );
    }

    public void testStaticMethodsNotInherited() throws IOException {
        Map<ModuleClass, List<AccessibleMethod>> methodsByClass = AccessibleJdkMethods.loadAccessibleMethods(Utils.DEFAULT_MODULE_PREDICATE)
            .collect(Collectors.groupingBy(Tuple::v1, Collectors.mapping(Tuple::v2, Collectors.toList())));

        var inputStreamMethods = methodsByClass.get(new ModuleClass("java.base", "java/io/InputStream"));
        var bufferedInputStreamMethods = methodsByClass.get(new ModuleClass("java.base", "java/io/BufferedInputStream"));

        Set<String> parentStaticMethods = inputStreamMethods.stream()
            .filter(m -> m.descriptor().isStatic())
            .map(m -> m.descriptor().method())
            .collect(Collectors.toSet());
        Set<String> childStaticMethods = bufferedInputStreamMethods.stream()
            .filter(m -> m.descriptor().isStatic())
            .map(m -> m.descriptor().method())
            .collect(Collectors.toSet());

        assertTrue("Static methods should not be inherited from parent", Collections.disjoint(parentStaticMethods, childStaticMethods));
    }
}
