package org.elasticsearch.index.query;

import org.apache.lucene.queryparser.XSimpleQueryParser;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Strings;

import java.util.Locale;

/**
 * Flags for the XSimpleQueryString parser
 */
public enum SimpleQueryStringFlag {
    ALL(-1),
    NONE(0),
    AND(XSimpleQueryParser.AND_OPERATOR),
    NOT(XSimpleQueryParser.NOT_OPERATOR),
    OR(XSimpleQueryParser.OR_OPERATOR),
    PREFIX(XSimpleQueryParser.PREFIX_OPERATOR),
    PRECEDENCE(XSimpleQueryParser.PRECEDENCE_OPERATORS),
    ESCAPE(XSimpleQueryParser.ESCAPE_OPERATOR),
    WHITESPACE(XSimpleQueryParser.WHITESPACE_OPERATOR);

    final int value;

    private SimpleQueryStringFlag(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    static int resolveFlags(String flags) {
        if (!Strings.hasLength(flags)) {
            return ALL.value();
        }
        int magic = NONE.value();
        for (String s : Strings.delimitedListToStringArray(flags, "|")) {
            if (s.isEmpty()) {
                continue;
            }
            try {
                SimpleQueryStringFlag flag = SimpleQueryStringFlag.valueOf(s.toUpperCase(Locale.ROOT));
                switch (flag) {
                    case NONE:
                        return 0;
                    case ALL:
                        return -1;
                    default:
                        magic |= flag.value();
                }
            } catch (IllegalArgumentException iae) {
                throw new ElasticSearchIllegalArgumentException("Unknown " + SimpleQueryStringParser.NAME + " flag [" + s + "]");
            }
        }
        return magic;
    }
}