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

import org.gradle.api.Action;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.FileCopyDetails;
import org.gradle.api.internal.file.copy.CopySpecInternal;
import org.gradle.api.internal.file.copy.CopySpecWrapper;
import org.gradle.api.internal.file.copy.DefaultCopySpec;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Packaging metadata (owner, group, rpm file type, directory-entry flags, ...) attached to a
 * Gradle {@link CopySpec}. Gradle's copy specs have no notion of these attributes, so they are
 * stored in a no-op {@link Action} that is registered on the spec via {@link CopySpec#eachFile}.
 * <p>
 * Storing the attributes <em>inside</em> the spec (rather than in a side table keyed by spec
 * instance, or in per-instance Groovy metaclass state as the original nebula plugin did) is what
 * makes this configuration-cache safe: the copy actions of a spec are serialized into the
 * configuration cache together with the task's copy spec hierarchy, so the attributes survive a
 * cache reuse where no configuration code runs.
 */
public final class SpecAttributes implements Action<FileCopyDetails>, Serializable {
    private static final long serialVersionUID = 1L;

    /** Attribute keys. */
    public static final String USER = "user";
    public static final String PERMISSION_GROUP = "permissionGroup";
    public static final String SETGID = "setgid";
    public static final String FILE_TYPE = "fileType";
    public static final String CREATE_DIRECTORY_ENTRY = "createDirectoryEntry";
    public static final String ADD_PARENT_DIRS = "addParentDirs";

    private final Map<String, Object> values = new LinkedHashMap<>();

    private SpecAttributes() {}

    @Override
    public void execute(FileCopyDetails fileCopyDetails) {
        // intentionally a no-op: this action only serves as a serializable attribute carrier
    }

    /**
     * Records an attribute on the given spec, attaching the carrier action if this is the first
     * attribute recorded for the spec.
     */
    public static void set(CopySpec spec, String key, Object value) {
        CopySpec target = unwrap(spec);
        SpecAttributes attributes = find(target);
        if (attributes == null) {
            attributes = new SpecAttributes();
            target.eachFile(attributes);
        }
        attributes.values.put(key, value);
    }

    /**
     * Looks up an attribute on the exact spec a file or directory was defined in. Returns
     * {@code null} when the attribute was not set on that spec; inheritance from the task-wide
     * defaults is handled by the callers.
     */
    public static Object lookup(CopySpecInternal spec, String key) {
        if (spec == null) {
            return null;
        }
        SpecAttributes attributes = find(spec);
        return attributes == null ? null : attributes.values.get(key);
    }

    private static SpecAttributes find(CopySpec spec) {
        if (spec instanceof DefaultCopySpec defaultCopySpec) {
            for (Action<? super FileCopyDetails> action : defaultCopySpec.getCopyActions()) {
                if (action instanceof SpecAttributes specAttributes) {
                    return specAttributes;
                }
            }
        }
        return null;
    }

    /**
     * Gradle hands {@link CopySpecWrapper} instances to configuration actions. The attribute
     * carrier must live on the underlying {@link DefaultCopySpec} because that is the instance
     * resolvable from {@code FileCopyDetails} at execution time.
     */
    private static CopySpec unwrap(CopySpec spec) {
        if (spec instanceof CopySpecWrapper) {
            try {
                Field delegateField = CopySpecWrapper.class.getDeclaredField("delegate");
                delegateField.setAccessible(true);
                return (CopySpec) delegateField.get(spec);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Cannot unwrap CopySpecWrapper", e);
            }
        }
        return spec;
    }
}
