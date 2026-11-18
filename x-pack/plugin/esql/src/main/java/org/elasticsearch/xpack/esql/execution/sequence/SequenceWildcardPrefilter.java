/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLikeList;

import java.util.List;

import static org.elasticsearch.xpack.esql.core.util.StringUtils.luceneWildcardToRegExp;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.CANDIDATE_TRANSLATOR_HANDLER;

/** A necessary index predicate. The original WHERE evaluates every fetched candidate. */
final class SequenceWildcardPrefilter {
    private static final String NON_ASCII = ".*[^\0-\177].*";
    private static final int MAX_REGEXP_LENGTH = 1_000;
    private static final int MAX_ENCODED_LENGTH = 2_048;

    private SequenceWildcardPrefilter() {}

    /** Called for positive AND conjuncts whose resolved attributes all belong to the selected source. */
    @Nullable
    static RegexpQueryBuilder fromConjunct(Expression conjunct) {
        if (conjunct instanceof RegexMatch<?> like
            && like.caseInsensitive() == false
            && like.field() instanceof ToLower lower
            && lower.field() instanceof FieldAttribute field
            && field.dataType() == DataType.KEYWORD) {
            List<WildcardPattern> patterns;
            if (like instanceof WildcardLike single) {
                patterns = List.of(single.pattern());
            } else if (like instanceof WildcardLikeList list) {
                patterns = list.pattern().patternList();
            } else {
                return null;
            }
            String regex = regexp(patterns);
            String fieldName = CANDIDATE_TRANSLATOR_HANDLER.nameOf(field.exactAttribute());
            // Preserve the old descriptor's cap, including field bytes and its three separators.
            if (regex == null || BytesRefs.toBytesRef(fieldName).length + 3L + regex.length() > MAX_ENCODED_LENGTH / 4 * 3) {
                return null;
            }
            return new RegexpQueryBuilder(fieldName, regex).flags(RegexpFlag.NONE);
        }
        return null;
    }

    @Nullable
    private static String regexp(List<WildcardPattern> patterns) {
        // TO_LOWER is locale-sensitive and can expand one code point to several. Fold only ASCII letters and
        // pass every non-ASCII value to the original evaluator, which also restores the actual null semantics.
        var regex = new StringBuilder(NON_ASCII);
        for (var pattern : patterns) {
            String wildcard = pattern.pattern();
            if (eligiblePattern(wildcard) == false) {
                return null;
            }
            regex.append("|(");
            for (char ch : luceneWildcardToRegExp(wildcard).toCharArray()) {
                if (ch >= 'a' && ch <= 'z') {
                    regex.append('[').append(ch).append((char) (ch - 'a' + 'A')).append(']');
                } else {
                    regex.append(ch);
                }
            }
            regex.append(')');
            if (regex.length() > MAX_REGEXP_LENGTH) {
                return null;
            }
        }
        return regex.toString();
    }

    private static boolean eligiblePattern(String wildcard) {
        if (wildcard.isEmpty() || wildcard.length() > MAX_REGEXP_LENGTH) {
            return false;
        }
        for (int i = 0; i < wildcard.length(); i++) {
            char ch = wildcard.charAt(i);
            if (ch < ' ' || ch > '~' || ch >= 'A' && ch <= 'Z') {
                return false;
            }
            if (ch == '\\') {
                // An unnecessary wildcard escape can become a metacharacter on conversion to regexp.
                if (++i == wildcard.length() || "\\*?".indexOf(wildcard.charAt(i)) < 0) {
                    return false;
                }
            }
        }
        return true;
    }
}
