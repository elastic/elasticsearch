/*
 * @notice
 * Copyright 2011-2019 the original author or authors.
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is derived from the nebula gradle-ospackage-plugin
 * (https://github.com/nebula-plugins/gradle-ospackage-plugin), reduced to the
 * functionality used by the Elasticsearch build and rewritten in Java.
 */

package org.elasticsearch.gradle.internal.ospackage;

import groovy.lang.Closure;
import groovy.lang.GroovyObjectSupport;

import org.codehaus.groovy.runtime.InvokerHelper;
import org.gradle.api.file.ConfigurableFilePermissions;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.DuplicatesStrategy;
import org.redline_rpm.payload.Directive;

/**
 * The delegate used when configuring {@code from(...) { ... }} / {@code into(...) { ... }} blocks
 * of a packaging task. It adds the packaging-only spec attributes ({@code user},
 * {@code permissionGroup}, {@code setgid}, {@code fileType}, {@code createDirectoryEntry}) on top
 * of a regular Gradle {@link CopySpec} and forwards everything else dynamically to the wrapped
 * (decorated) spec. Nested {@code from}/{@code into} blocks are wrapped again so the extra
 * attributes are available at any nesting level.
 */
public class EnhancedCopySpec extends GroovyObjectSupport {

    private final CopySpec delegate;
    private final SystemPackagingTask task;

    private EnhancedCopySpec(CopySpec delegate, SystemPackagingTask task) {
        this.delegate = delegate;
        this.task = task;
    }

    /**
     * Configures the given closure against an enhanced wrapper of {@code spec} using Gradle's
     * usual delegate-first resolution. The {@code task} back-reference resolves the task-level DSL
     * (e.g. execution-time {@code directory(...)} registration from {@code eachFile} callbacks)
     * deterministically inside nested spec closures.
     */
    public static void configure(Closure<?> closure, CopySpec spec, SystemPackagingTask task) {
        // packaging never wants "duplicate file" failures; mirror the original plugin behavior
        spec.setDuplicatesStrategy(DuplicatesStrategy.INCLUDE);
        EnhancedCopySpec enhanced = new EnhancedCopySpec(spec, task);
        Closure<?> copy = (Closure<?>) closure.clone();
        copy.setResolveStrategy(Closure.DELEGATE_FIRST);
        copy.setDelegate(enhanced);
        if (copy.getMaximumNumberOfParameters() == 0) {
            copy.call();
        } else {
            copy.call(enhanced);
        }
    }

    // ----------------------------------------------------------------------
    // packaging attributes
    // ----------------------------------------------------------------------

    public void user(String user) {
        SpecAttributes.set(delegate, SpecAttributes.USER, user);
    }

    public void permissionGroup(String permissionGroup) {
        SpecAttributes.set(delegate, SpecAttributes.PERMISSION_GROUP, permissionGroup);
    }

    public void setgid(boolean setgid) {
        SpecAttributes.set(delegate, SpecAttributes.SETGID, setgid);
    }

    /** RPM only: marks files of this spec with the given raw {@code Directive#RPMFILE_*} flag bits. */
    public void fileType(int fileTypeFlags) {
        SpecAttributes.set(delegate, SpecAttributes.FILE_TYPE, new Directive(fileTypeFlags));
    }

    public void createDirectoryEntry(boolean createDirectoryEntry) {
        SpecAttributes.set(delegate, SpecAttributes.CREATE_DIRECTORY_ENTRY, createDirectoryEntry);
    }

    /**
     * Registers a package-owned directory entry on the owning task. Exposed here so that
     * {@code directory(...)} calls from {@code eachFile} callbacks nested in spec closures resolve
     * without relying on Groovy owner-chain fallthrough.
     */
    public Directory directory(String path, int permissions) {
        return task.directory(path, permissions);
    }

    // ----------------------------------------------------------------------
    // nested copy spec configuration: re-wrap children so the packaging
    // attributes stay available in nested blocks
    // ----------------------------------------------------------------------

    public CopySpec from(Object sourcePath, Closure<?> closure) {
        return delegate.from(sourcePath, child -> configure(closure, child, task));
    }

    public CopySpec into(Object destPath, Closure<?> closure) {
        return delegate.into(destPath, child -> configure(closure, child, task));
    }

    /**
     * Explicit {@link Closure} overloads for the permission blocks: unlike {@code eachFile} or
     * {@code into} these have no Closure variant on the {@link CopySpec} interface, and the
     * wrapped spec instance is not guaranteed to be a Gradle-decorated object providing one.
     */
    public void filePermissions(Closure<?> closure) {
        delegate.filePermissions(permissions -> configurePermissions(closure, permissions));
    }

    public void dirPermissions(Closure<?> closure) {
        delegate.dirPermissions(permissions -> configurePermissions(closure, permissions));
    }

    private static void configurePermissions(Closure<?> closure, ConfigurableFilePermissions permissions) {
        Closure<?> copy = (Closure<?>) closure.clone();
        copy.setResolveStrategy(Closure.DELEGATE_FIRST);
        copy.setDelegate(permissions);
        if (copy.getMaximumNumberOfParameters() == 0) {
            copy.call();
        } else {
            copy.call(permissions);
        }
    }

    // ----------------------------------------------------------------------
    // dynamic forwarding of the regular CopySpec API
    // ----------------------------------------------------------------------

    @Override
    public Object invokeMethod(String name, Object args) {
        return InvokerHelper.invokeMethod(delegate, name, args);
    }

    @Override
    public Object getProperty(String property) {
        return InvokerHelper.getProperty(delegate, property);
    }

    @Override
    public void setProperty(String property, Object newValue) {
        // support assignment style for the setgid attribute (`setgid = true`), used by the
        // Elasticsearch packaging build; everything else is a regular CopySpec property
        if (SpecAttributes.SETGID.equals(property)) {
            setgid((Boolean) newValue);
        } else {
            InvokerHelper.setProperty(delegate, property, newValue);
        }
    }
}
