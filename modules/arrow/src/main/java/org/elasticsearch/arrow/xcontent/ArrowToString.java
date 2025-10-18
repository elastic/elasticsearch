/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.xcontent;

import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VariableWidthFieldVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.elasticsearch.libs.arrow.ArrowFormatException;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ArrowToString {

    public static String getString(ValueVector vector, int position, Map<Long, Dictionary> dictionaries) {
        if (vector.isNull(position)) {
            return null;
        }

        return switch (vector.getMinorType()) {

            case VARCHAR, LARGEVARCHAR, VIEWVARCHAR -> {
                var bytesVector = (VariableWidthFieldVector) vector;
                yield new String(bytesVector.get(position), StandardCharsets.UTF_8);
            }

            case TINYINT, SMALLINT, INT, BIGINT, UINT1, UINT2, UINT4, UINT8 -> String.valueOf(
                ((BaseIntVector) vector).getValueAsLong(position)
            );

            case UNION -> {
                UnionVector unionVector = (UnionVector) vector;
                // Find the child field that isn't null, which is the active variant.
                for (var variantVec : unionVector.getChildrenFromFields()) {
                    if (variantVec.isNull(position) == false) {
                        yield getString(variantVec, position, dictionaries);
                    }
                }
                yield null;
            }

            default -> {
                throw new ArrowFormatException("Arrow type [" + vector.getMinorType() + "] cannot be converted to string");
            }
        };
    }
}
