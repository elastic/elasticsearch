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

import org.redline_rpm.header.Flags;

import java.io.Serializable;

/**
 * A package relationship (requires/conflicts/obsoletes) shared by the rpm and deb models. The
 * {@code flag} carries redline {@link Flags} semantics and is translated to the debian comparison
 * operators when rendered into a deb control file.
 */
public record Dependency(String packageName, String version, int flag) implements Serializable {

    public Dependency {
        if (packageName.contains(",")) {
            throw new IllegalArgumentException("Package name (" + packageName + ") can not include commas");
        }
    }

    public String toDebString() {
        StringBuilder depStr = new StringBuilder(packageName);
        if (flag != 0 && version != null && version.isEmpty() == false) {
            depStr.append(" (").append(debianComparison(flag)).append(' ').append(version).append(')');
        } else if (version != null && version.isEmpty() == false) {
            depStr.append(" (").append(version).append(')');
        }
        return depStr.toString();
    }

    private static String debianComparison(int flag) {
        if (flag == (Flags.GREATER | Flags.EQUAL)) {
            return ">=";
        } else if (flag == (Flags.LESS | Flags.EQUAL)) {
            return "<=";
        } else if (flag == Flags.EQUAL) {
            return "=";
        } else if (flag == Flags.GREATER) {
            return ">>";
        } else if (flag == Flags.LESS) {
            return "<<";
        } else {
            throw new IllegalArgumentException("Unsupported dependency comparison flag [" + flag + "]");
        }
    }
}
