/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.rules.DeniedEntitlementStrategy;
import org.elasticsearch.entitlement.rules.EntitlementRule;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.nio.file.FileSystems;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.StreamSupport;

public class PathInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        // Path implementations are platform-specific, so we need to dynamically determine the class names
        StreamSupport.stream(FileSystems.getDefault().getRootDirectories().spliterator(), false)
            .map(Path::getClass)
            .map(Class::getName)
            .map(className -> className.replace('.', '/'))
            .distinct()
            .forEach(className -> {
                registry.registerRule(
                    new EntitlementRule(new MethodKey(className, "toRealPath", List.of("java.nio.file.LinkOption[]")), args -> {
                        Path path = (Path) args[0];
                        LinkOption[] options = (LinkOption[]) args[1];

                        boolean followLinks = true;
                        for (LinkOption option : options) {
                            if (option == LinkOption.NOFOLLOW_LINKS) {
                                followLinks = false;
                            }
                        }
                        return Policies.fileReadWithLinks(path, followLinks);
                    }, new DeniedEntitlementStrategy.NotEntitledDeniedEntitlementStrategy())
                );

                registry.registerRule(
                    new EntitlementRule(
                        new MethodKey(
                            className,
                            "register",
                            List.of("java.nio.file.WatchService", "java.nio.file.WatchEvent$Kind[]", "java.nio.file.WatchEvent$Modifier[]")
                        ),
                        args -> {
                            Path path = (Path) args[0];
                            return Policies.fileRead(path);
                        },
                        new DeniedEntitlementStrategy.NotEntitledDeniedEntitlementStrategy()
                    )
                );
            });

        registry.registerRule(
            new EntitlementRule(
                new MethodKey("java/nio/file/Path", "register", List.of("java.nio.file.WatchService", "java.nio.file.WatchEvent$Kind[]")),
                args -> {
                    Path path = (Path) args[0];
                    return Policies.fileRead(path);
                },
                new DeniedEntitlementStrategy.NotEntitledDeniedEntitlementStrategy()
            )
        );
    }
}
