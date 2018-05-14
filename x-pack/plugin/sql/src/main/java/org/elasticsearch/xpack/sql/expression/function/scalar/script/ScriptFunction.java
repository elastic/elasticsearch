/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 *  [2017] Elasticsearch Incorporated. All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Elasticsearch Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Elasticsearch Incorporated.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.script;

import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;

public class ScriptFunction {

    final String name;
    final String definition;

    public ScriptFunction(String name, String definition) {
        this.name = name;
        this.definition = definition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, definition);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ScriptFunction other = (ScriptFunction) obj;
        return Objects.equals(name, other.name) && Objects.equals(definition, other.definition);
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "%s=%s", name, definition);
    }
}