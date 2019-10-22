package org.elasticsearch.xpack.security.authc.saml;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * A formatter that allows named placeholders e.g. "%(var_name)" to be replaced.
 * Unlike the original implementation, parameters without values will cause am
 * {@link IllegalArgumentException} to be thrown.
 *
 * @see <a href="https://codereview.stackexchange.com/questions/194446/messageformat-format-with-named-parameters"
 *       >Original post on Stack Exchange</a>.
 */
public class NamedFormatter {
    private static final Pattern RE = Pattern.compile(
        // Treat any character after a backslash literally, and look for %(keys) to replace
        "\\\\(.) | (%\\(([^)]+)\\))",
        Pattern.COMMENTS
    );

    private NamedFormatter() {}

    /**
     * Expands format strings containing <code>%(keys)</code>.
     *
     * <p>Examples:</p>
     *
     * <ul>
     * <li><code>NamedFormatter.format("Hello, %(name)!", Map.of("name", "200_success"))</code> → <code>"Hello, 200_success!"</code></li>
     * <li><code>NamedFormatter.format("Hello, \%(name)!", Map.of("name", "200_success"))</code> → <code>"Hello, %(name)!"</code></li>
     * <li><code>NamedFormatter.format("Hello, %(name)!", Map.of("foo", "bar"))</code> → {@link IllegalArgumentException}</li>
     * </ul>
     *
     * @param fmt The format string.  Any character in the format string that
     *            follows a backslash is treated literally.  Any
     *            <code>%(key)</code> is replaced by its corresponding value
     *            in the <code>values</code> map.  If the key does not exist
     *            in the <code>values</code> map, then it is left unsubstituted.
     * @param values Key-value pairs to be used in the substitutions.
     * @return The formatted string.
     */
    public static String format(String fmt, Map<String, Object> values) {
        return RE.matcher(fmt)
            .replaceAll(match -> {
                // Escaped characters are unchanged
                if (match.group(1) != null) {
                    return match.group(1);
                }

                final String paramName = match.group(3);
                if (values.containsKey(paramName)) {
                    return values.get(paramName).toString();
                }

                throw new IllegalArgumentException("No parameter value for %(" + paramName + ")");
            });
    }
}
