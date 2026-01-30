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
import org.elasticsearch.entitlement.rules.EntitlementHandler;
import org.elasticsearch.entitlement.rules.EntitlementRule;
import org.elasticsearch.entitlement.rules.Policies;

import java.nio.file.FileSystems;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

public class PathInstrumentation implements InstrumentationConfig {
    @Override
    public void init(Consumer<EntitlementRule> addRule) {
        var pathClasses = StreamSupport.stream(FileSystems.getDefault().getRootDirectories().spliterator(), false)
            .map(Path::getClass)
            .distinct()
            .toList();

        // EntitlementRules.on(pathClasses, c -> c.calling(Path::toRealPath, LinkOption[].class).enforce((path, options) -> {
        // boolean followLinks = true;
        // for (LinkOption option : options) {
        // if (option == LinkOption.NOFOLLOW_LINKS) {
        // followLinks = false;
        // break;
        // }
        // }
        // return Policies.fileReadWithLinks(path, followLinks);
        // })
        // .elseThrowNotEntitled()
        // .calling(Path::register, TypeToken.of(WatchService.class), new TypeToken<WatchEvent.Kind<?>>() {})
        // .enforce(Policies::fileRead)
        // .elseThrowNotEntitled()
        // .calling(
        // Path::register,
        // TypeToken.of(WatchService.class),
        // new TypeToken<WatchEvent.Kind<?>[]>() {},
        // TypeToken.of(WatchEvent.Modifier.class)
        // )
        // .enforce(Policies::fileRead)
        // .elseThrowNotEntitled());

        addRule.accept(
            new EntitlementRule(new MethodKey("sun/nio/fs/UnixPath", "toRealPath", List.of("java.nio.file.LinkOption[]")), args -> {
                Path path = (Path) args[0];
                LinkOption[] options = (LinkOption[]) args[1];

                boolean followLinks = true;
                for (LinkOption option : options) {
                    if (option == LinkOption.NOFOLLOW_LINKS) {
                        followLinks = false;
                    }
                }
                return Policies.fileReadWithLinks(path, followLinks);
            }, new EntitlementHandler.NotEntitledEntitlementHandler())
        );

        addRule.accept(
            new EntitlementRule(
                new MethodKey("java/nio/file/Path", "register", List.of("java.nio.file.WatchService", "java.nio.file.WatchEvent$Kind[]")),
                args -> {
                    Path path = (Path) args[0];
                    return Policies.fileRead(path);
                },
                new EntitlementHandler.NotEntitledEntitlementHandler()
            )
        );

        addRule.accept(
            new EntitlementRule(
                new MethodKey(
                    "sun/nio/fs/UnixPath",
                    "register",
                    List.of("java.nio.file.WatchService", "java.nio.file.WatchEvent$Kind[]", "java.nio.file.WatchEvent$Modifier[]")
                ),
                args -> {
                    Path path = (Path) args[0];
                    return Policies.fileRead(path);
                },
                new EntitlementHandler.NotEntitledEntitlementHandler()
            )
        );
    }
}
