package graphql.parser;

import graphql.Assert;
import graphql.Internal;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Contains parsing code for the StringValue types in the grammar
 */
@Internal
public class StringValueParsing {
    private final static String ESCAPED_TRIPLE_QUOTES = "\\\\\"\"\""; // ahh Java + Regex
    private final static String THREE_QUOTES = "\"\"\"";

    public static String parseTripleQuotedString(String strText) {
        int end = strText.length() - 3;
        String s = strText.substring(3, end);
        s = s.replaceAll(ESCAPED_TRIPLE_QUOTES, THREE_QUOTES);
        return removeIndentation(s);
    }

    /*
       See https://github.com/facebook/graphql/pull/327/files#diff-fe406b08746616e2f5f00909488cce66R758
     */
    public static String removeIndentation(String rawValue) {
        String[] lines = rawValue.split("\\n");
        Integer commonIndent = null;
        for (int i = 0; i < lines.length; i++) {
            if (i == 0) continue;
            String line = lines[i];
            int length = line.length();
            int indent = leadingWhitespace(line);
            if (indent < length) {
                if (commonIndent == null || indent < commonIndent) {
                    commonIndent = indent;
                }
            }
        }
        List<String> lineList = new ArrayList<>(Arrays.asList(lines));
        if (commonIndent != null) {
            for (int i = 0; i < lineList.size(); i++) {
                String line = lineList.get(i);
                if (i == 0) continue;
                if (line.length() > commonIndent) {
                    line = line.substring(commonIndent);
                    lineList.set(i, line);
                }
            }
        }
        while (!lineList.isEmpty()) {
            String line = lineList.get(0);
            if (containsOnlyWhiteSpace(line)) {
                lineList.remove(0);
            } else {
                break;
            }
        }
        while (!lineList.isEmpty()) {
            int endIndex = lineList.size() - 1;
            String line = lineList.get(endIndex);
            if (containsOnlyWhiteSpace(line)) {
                lineList.remove(endIndex);
            } else {
                break;
            }
        }
        StringBuilder formatted = new StringBuilder();
        for (int i = 0; i < lineList.size(); i++) {
            String line = lineList.get(i);
            if (i == 0) {
                formatted.append(line);
            } else {
                formatted.append("\n");
                formatted.append(line);
            }
        }
        return formatted.toString();
    }

    private static int leadingWhitespace(String str) {
        int count = 0;
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (ch != ' ' && ch != '\t') {
                break;
            }
            count++;
        }
        return count;
    }

    private static boolean containsOnlyWhiteSpace(String str) {
        // according to graphql spec and graphql-js - this is the definition
        return leadingWhitespace(str) == str.length();
    }

    public static String parseSingleQuotedString(String string) {
        StringWriter writer = new StringWriter(string.length() - 2);
        int end = string.length() - 1;
        for (int i = 1; i < end; i++) {
            char c = string.charAt(i);
            if (c != '\\') {
                writer.write(c);
                continue;
            }
            char escaped = string.charAt(i + 1);
            i += 1;
            switch (escaped) {
                case '"':
                    writer.write('"');
                    continue;
                case '/':
                    writer.write('/');
                    continue;
                case '\\':
                    writer.write('\\');
                    continue;
                case 'b':
                    writer.write('\b');
                    continue;
                case 'f':
                    writer.write('\f');
                    continue;
                case 'n':
                    writer.write('\n');
                    continue;
                case 'r':
                    writer.write('\r');
                    continue;
                case 't':
                    writer.write('\t');
                    continue;
                case 'u':
                    String hexStr = string.substring(i + 1, i + 5);
                    int codepoint = Integer.parseInt(hexStr, 16);
                    i += 4;
                    writer.write(codepoint);
                    continue;
                default:
                    Assert.assertShouldNeverHappen();
            }
        }
        return writer.toString();
    }
}
