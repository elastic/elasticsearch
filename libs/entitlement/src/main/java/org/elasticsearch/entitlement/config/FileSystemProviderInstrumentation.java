/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.rules.TypeToken;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.net.URI;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class FileSystemProviderInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);

        builder.on(FileSystemProvider.class).protectedCtor().enforce(Policies::changeJvmGlobalState).elseThrowNotEntitled();

        builder.on(FileSystems.getDefault().provider().getClass(), rule -> {
            rule.calling(FileSystemProvider::newFileSystem, TypeToken.of(URI.class), new TypeToken<Map<String, ?>>() {})
                .enforce(Policies::changeJvmGlobalState)
                .elseThrowNotEntitled();
            rule.calling(FileSystemProvider::newFileSystem, TypeToken.of(Path.class), new TypeToken<Map<String, ?>>() {})
                .enforce(Policies::changeJvmGlobalState)
                .elseThrowNotEntitled();
            rule.calling(FileSystemProvider::newInputStream, Path.class, OpenOption[].class)
                .enforce((_, path) -> Policies.fileRead(path))
                .elseThrowNotEntitled();
            rule.calling(FileSystemProvider::newOutputStream, Path.class, OpenOption[].class)
                .enforce((_, path) -> Policies.fileWrite(path))
                .elseThrowNotEntitled();
            rule.calling(
                FileSystemProvider::newFileChannel,
                TypeToken.of(Path.class),
                new TypeToken<Set<OpenOption>>() {},
                TypeToken.of(FileAttribute[].class)
            ).enforce((_, path, options) -> Policies.fileReadOrWrite(path, options)).elseThrowNotEntitled();
            rule.calling(
                FileSystemProvider::newAsynchronousFileChannel,
                TypeToken.of(Path.class),
                new TypeToken<Set<OpenOption>>() {},
                TypeToken.of(ExecutorService.class),
                TypeToken.of(FileAttribute[].class)
            ).enforce((_, path, options) -> Policies.fileReadOrWrite(path, options)).elseThrowNotEntitled();
            rule.calling(
                FileSystemProvider::newByteChannel,
                TypeToken.of(Path.class),
                new TypeToken<Set<? extends OpenOption>>() {},
                TypeToken.of(FileAttribute[].class)
            ).enforce((_, path, options) -> Policies.fileReadOrWrite(path, options)).elseThrowNotEntitled();
            rule.calling(
                FileSystemProvider::newDirectoryStream,
                TypeToken.of(Path.class),
                new TypeToken<DirectoryStream.Filter<? super Path>>() {}
            ).enforce((_, path) -> Policies.fileRead(path)).elseThrowNotEntitled();
            rule.callingVoid(FileSystemProvider::createDirectory, Path.class, FileAttribute[].class)
                .enforce((_, path) -> Policies.fileWrite(path))
                .elseThrowNotEntitled();
            rule.callingVoid(FileSystemProvider::createSymbolicLink, Path.class, Path.class, FileAttribute[].class)
                .enforce((_, link, target) -> Policies.fileWrite(link).and(Policies.fileRead(target)))
                .elseThrowNotEntitled();
            rule.callingVoid(FileSystemProvider::createLink, Path.class, Path.class)
                .enforce((_, link, target) -> Policies.fileWrite(link).and(Policies.fileRead(target)))
                .elseThrowNotEntitled();
            rule.callingVoid(FileSystemProvider::delete, Path.class).enforce((_, path) -> Policies.fileWrite(path)).elseThrowNotEntitled();
            rule.calling(FileSystemProvider::deleteIfExists, Path.class)
                .enforce((_, path) -> Policies.fileWrite(path))
                .elseThrowNotEntitled();
            rule.calling(FileSystemProvider::readSymbolicLink, Path.class)
                .enforce((_, path) -> Policies.fileRead(path))
                .elseThrowNotEntitled();
            rule.callingVoid(FileSystemProvider::copy, Path.class, Path.class, CopyOption[].class)
                .enforce((_, source, target) -> Policies.fileRead(source).and(Policies.fileWrite(target)))
                .elseThrowNotEntitled();
            rule.callingVoid(FileSystemProvider::move, Path.class, Path.class, CopyOption[].class)
                .enforce((_, source, target) -> Policies.fileRead(source).and(Policies.fileWrite(target)))
                .elseThrowNotEntitled();
            rule.calling(FileSystemProvider::isSameFile, Path.class, Path.class)
                .enforce((_, path, path2) -> Policies.fileRead(path).and(Policies.fileRead(path2)))
                .elseThrowNotEntitled();
            rule.calling(FileSystemProvider::isHidden, Path.class).enforce((_, path) -> Policies.fileRead(path)).elseThrowNotEntitled();
            rule.calling(FileSystemProvider::getFileStore, Path.class).enforce((_, path) -> Policies.fileRead(path)).elseThrowNotEntitled();
            rule.callingVoid(FileSystemProvider::checkAccess, Path.class, AccessMode[].class)
                .enforce((_, path) -> Policies.fileRead(path))
                .elseThrowNotEntitled();
            rule.calling(
                FileSystemProvider::getFileAttributeView,
                TypeToken.of(Path.class),
                new TypeToken<Class<FileAttributeView>>() {},
                TypeToken.of(LinkOption[].class)
            ).enforce(Policies::getFileAttributeView).elseThrowNotEntitled();
            rule.calling(
                FileSystemProvider::readAttributes,
                TypeToken.of(Path.class),
                new TypeToken<Class<BasicFileAttributes>>() {},
                TypeToken.of(LinkOption[].class)
            ).enforce((_, path) -> Policies.fileRead(path)).elseThrowNotEntitled();
            rule.calling(FileSystemProvider::readAttributes, Path.class, String.class, LinkOption[].class)
                .enforce((_, path) -> Policies.fileRead(path))
                .elseThrowNotEntitled();
            rule.calling(
                FileSystemProvider::readAttributesIfExists,
                TypeToken.of(Path.class),
                new TypeToken<Class<BasicFileAttributes>>() {},
                TypeToken.of(LinkOption[].class)
            ).enforce((_, path) -> Policies.fileRead(path)).elseThrowNotEntitled();
            rule.callingVoid(FileSystemProvider::setAttribute, Path.class, String.class, Object.class, LinkOption[].class)
                .enforce((_, path) -> Policies.fileWrite(path))
                .elseThrowNotEntitled();
            rule.calling(FileSystemProvider::exists, Path.class, LinkOption[].class)
                .enforce((_, path) -> Policies.fileRead(path))
                .elseThrowNotEntitled();
        });
    }
}
