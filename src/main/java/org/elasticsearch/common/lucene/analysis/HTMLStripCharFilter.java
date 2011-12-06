/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.analysis;

import org.apache.lucene.analysis.BaseCharFilter;
import org.apache.lucene.analysis.CharReader;
import org.apache.lucene.analysis.CharStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Set;

/**
 *
 */
// LUCENE MONITOR: Once the next Lucene version is out, use the built in HTML filter
public class HTMLStripCharFilter extends BaseCharFilter {
    private int readAheadLimit = DEFAULT_READ_AHEAD;
    private int safeReadAheadLimit = readAheadLimit - 3;
    private int numWhitespace = 0;
    private int numRead = 0;
    private int numEaten = 0;
    private int numReturned = 0;
    private int lastMark;
    private Set<String> escapedTags;

    // pushback buffer
    private final StringBuilder pushed = new StringBuilder();
    private static final int EOF = -1;
    private static final int MISMATCH = -2;

    private static final int MATCH = -3;
    // temporary buffer
    private final StringBuilder sb = new StringBuilder();
    public static final int DEFAULT_READ_AHEAD = 8192;


    public static void main(String[] args) throws IOException {
        Reader in = new HTMLStripCharFilter(
                CharReader.get(new InputStreamReader(System.in)));
        int ch;
        while ((ch = in.read()) != -1) System.out.print((char) ch);
    }

    public HTMLStripCharFilter(CharStream source) {
        super(source.markSupported() ? source : CharReader.get(new BufferedReader(source)));
    }

    public HTMLStripCharFilter(CharStream source, Set<String> escapedTags) {
        this(source);
        this.escapedTags = escapedTags;
    }

    public HTMLStripCharFilter(CharStream source, Set<String> escapedTags, int readAheadLimit) {
        this(source);
        this.escapedTags = escapedTags;
        this.readAheadLimit = readAheadLimit;
        safeReadAheadLimit = readAheadLimit - 3;
    }

    public int getReadAheadLimit() {
        return readAheadLimit;
    }

    private int next() throws IOException {
        int len = pushed.length();
        if (len > 0) {
            int ch = pushed.charAt(len - 1);
            pushed.setLength(len - 1);
            return ch;
        }
        numRead++;
        return input.read();
    }

    private int nextSkipWS() throws IOException {
        int ch = next();
        while (isSpace(ch)) ch = next();
        return ch;
    }

    private int peek() throws IOException {
        int len = pushed.length();
        if (len > 0) {
            return pushed.charAt(len - 1);
        }
        int ch = input.read();
        push(ch);
        return ch;
    }

    private void push(int ch) {
        pushed.append((char) ch);
    }


    private boolean isSpace(int ch) {
        switch (ch) {
            case ' ':
            case '\n':
            case '\r':
            case '\t':
                return true;
            default:
                return false;
        }
    }

    private boolean isHex(int ch) {
        return (ch >= '0' && ch <= '9') ||
                (ch >= 'A' && ch <= 'Z') ||
                (ch >= 'a' && ch <= 'z');
    }

    private boolean isAlpha(int ch) {
        return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z';
    }

    private boolean isDigit(int ch) {
        return ch >= '0' && ch <= '9';
    }

    /**
     * From HTML 4.0
     * [4]     NameChar     ::=    Letter | Digit | '.' | '-' | '_' | ':' | CombiningChar | Extender
     * [5]     Name     ::=    (Letter | '_' | ':') (NameChar)*
     * [6]     Names    ::=    Name (#x20 Name)*
     * [7]     Nmtoken    ::=    (NameChar)+
     * [8]     Nmtokens     ::=    Nmtoken (#x20 Nmtoken)*
     * *
     */

    // should I include all id chars allowable by HTML/XML here?
    // including accented chars, ':', etc?
    private boolean isIdChar(int ch) {
        // return Character.isUnicodeIdentifierPart(ch);
        // isUnicodeIdentiferPart doesn't include '-'... shoudl I still
        // use it and add in '-',':',etc?
        return isAlpha(ch) || isDigit(ch) || ch == '.' ||
                ch == '-' || ch == '_' || ch == ':'
                || Character.isLetter(ch);

    }

    private boolean isFirstIdChar(int ch) {
        return Character.isUnicodeIdentifierStart(ch);
        // return isAlpha(ch) || ch=='_' || Character.isLetter(ch);
    }


    private void saveState() throws IOException {
        lastMark = numRead;
        input.mark(readAheadLimit);
    }

    private void restoreState() throws IOException {
        input.reset();
        pushed.setLength(0);
    }

    private int readNumericEntity() throws IOException {
        // "&#" has already been read at this point
        int eaten = 2;

        // is this decimal, hex, or nothing at all.
        int ch = next();
        int base = 10;
        boolean invalid = false;
        sb.setLength(0);

        if (isDigit(ch)) {
            // decimal character entity
            sb.append((char) ch);
            for (int i = 0; i < 10; i++) {
                ch = next();
                if (isDigit(ch)) {
                    sb.append((char) ch);
                } else {
                    break;
                }
            }
        } else if (ch == 'x') {
            eaten++;
            // hex character entity
            base = 16;
            sb.setLength(0);
            for (int i = 0; i < 10; i++) {
                ch = next();
                if (isHex(ch)) {
                    sb.append((char) ch);
                } else {
                    break;
                }
            }
        } else {
            return MISMATCH;
        }


        // In older HTML, an entity may not have always been terminated
        // with a semicolon.  We'll also treat EOF or whitespace as terminating
        // the entity.
        try {
            if (ch == ';' || ch == -1) {
                // do not account for the eaten ";" due to the fact that we do output a char
                numWhitespace = sb.length() + eaten;
                return Integer.parseInt(sb.toString(), base);
            }

            // if whitespace terminated the entity, we need to return
            // that whitespace on the next call to read().
            if (isSpace(ch)) {
                push(ch);
                numWhitespace = sb.length() + eaten;
                return Integer.parseInt(sb.toString(), base);
            }
        } catch (NumberFormatException e) {
            return MISMATCH;
        }

        // Not an entity...
        return MISMATCH;
    }

    private int readEntity() throws IOException {
        int ch = next();
        if (ch == '#') return readNumericEntity();

        //read an entity reference

        // for an entity reference, require the ';' for safety.
        // otherwise we may try and convert part of some company
        // names to an entity.  "Alpha&Beta Corp" for instance.
        //
        // TODO: perhaps I should special case some of the
        // more common ones like &amp to make the ';' optional...

        sb.setLength(0);
        sb.append((char) ch);

        for (int i = 0; i < safeReadAheadLimit; i++) {
            ch = next();
            if (Character.isLetter(ch)) {
                sb.append((char) ch);
            } else {
                break;
            }
        }

        if (ch == ';') {
            String entity = sb.toString();
            Character entityChar = entityTable.get(entity);
            if (entityChar != null) {
                numWhitespace = entity.length() + 1;
                return entityChar.charValue();
            }
        }

        return MISMATCH;
    }

    /**
     * valid comments according to HTML specs
     * <!-- Hello -->
     * <!-- Hello -- -- Hello-->
     * <!---->
     * <!------ Hello -->
     * <!>
     * <!------> Hello -->
     * <p/>
     * #comments inside of an entity decl:
     * <!ENTITY amp     CDATA "&#38;"   -- ampersand, U+0026 ISOnum -->
     * <p/>
     * Turns out, IE & mozilla don't parse comments correctly.
     * Since this is meant to be a practical stripper, I'll just
     * try and duplicate what the browsers do.
     * <p/>
     * <!-- (stuff_including_markup)* -->
     * <!FOO (stuff, not including markup) >
     * <! (stuff, not including markup)* >
     * <p/>
     * <p/>
     * *
     */

    private int readBang(boolean inScript) throws IOException {
        // at this point, "<!" has been read
        int ret = readComment(inScript);
        if (ret == MATCH) return MATCH;

        if ((numRead - lastMark) < safeReadAheadLimit || peek() == '>') {

            int ch = next();
            if (ch == '>') return MATCH;

            // if it starts with <! and isn't a comment,
            // simply read until ">"
            //since we did readComment already, it may be the case that we are already deep into the read ahead buffer
            //so, we may need to abort sooner
            while ((numRead - lastMark) < safeReadAheadLimit) {
                ch = next();
                if (ch == '>') {
                    return MATCH;
                } else if (ch < 0) {
                    return MISMATCH;
                }
            }
        }
        return MISMATCH;
    }

    // tries to read comments the way browsers do, not
    // strictly by the standards.
    //
    // GRRRR.  it turns out that in the wild, a <script> can have a HTML comment
    // that contains a script that contains a quoted comment.
    // <script><!-- document.write("<!--embedded comment-->") --></script>
    //

    private int readComment(boolean inScript) throws IOException {
        // at this point "<!" has  been read
        int ch = next();
        if (ch != '-') {
            // not a comment
            push(ch);
            return MISMATCH;
        }

        ch = next();
        if (ch != '-') {
            // not a comment
            push(ch);
            push('-');
            return MISMATCH;
        }
        /*two extra calls to next() here, so make sure we don't read past our mark*/
        while ((numRead - lastMark) < safeReadAheadLimit - 3) {
            ch = next();
            if (ch < 0) return MISMATCH;
            if (ch == '-') {
                ch = next();
                if (ch < 0) return MISMATCH;
                if (ch != '-') {
                    push(ch);
                    continue;
                }

                ch = next();
                if (ch < 0) return MISMATCH;
                if (ch != '>') {
                    push(ch);
                    push('-');
                    continue;
                }

                return MATCH;
            } else if ((ch == '\'' || ch == '"') && inScript) {
                push(ch);
                int ret = readScriptString();
                // if this wasn't a string, there's not much we can do
                // at this point without having a stack of stream states in
                // order to "undo" just the latest.
            } else if (ch == '<') {
                eatSSI();
            }

        }
        return MISMATCH;

    }


    private int readTag() throws IOException {
        // at this point '<' has already been read
        int ch = next();
        if (!isAlpha(ch)) {
            push(ch);
            return MISMATCH;
        }

        sb.setLength(0);
        sb.append((char) ch);
        while ((numRead - lastMark) < safeReadAheadLimit) {

            ch = next();
            if (isIdChar(ch)) {
                sb.append((char) ch);
            } else if (ch == '/') {
                // Hmmm, a tag can close with "/>" as well as "/ >"
                // read end tag '/>' or '/ >', etc
                return nextSkipWS() == '>' ? MATCH : MISMATCH;
            } else {
                break;
            }
        }
        if (escapedTags != null && escapedTags.contains(sb.toString())) {
            //if this is a reservedTag, then keep it
            return MISMATCH;
        }
        // After the tag id, there needs to be either whitespace or
        // '>'
        if (!(ch == '>' || isSpace(ch))) {
            return MISMATCH;
        }

        if (ch != '>') {
            // process attributes
            while ((numRead - lastMark) < safeReadAheadLimit) {
                ch = next();
                if (isSpace(ch)) {
                    continue;
                } else if (isFirstIdChar(ch)) {
                    push(ch);
                    int ret = readAttr2();
                    if (ret == MISMATCH) return ret;
                } else if (ch == '/') {
                    // read end tag '/>' or '/ >', etc
                    return nextSkipWS() == '>' ? MATCH : MISMATCH;
                } else if (ch == '>') {
                    break;
                } else {
                    return MISMATCH;
                }

            }
            if ((numRead - lastMark) >= safeReadAheadLimit) {
                return MISMATCH;//exit out if we exceeded the buffer
            }
        }

        // We only get to this point after we have read the
        // entire tag.  Now let's see if it's a special tag.
        String name = sb.toString();
        if (name.equalsIgnoreCase("script") || name.equalsIgnoreCase("style")) {
            // The content of script and style elements is
            //  CDATA in HTML 4 but PCDATA in XHTML.

            /* From HTML4:
             Although the STYLE and SCRIPT elements use CDATA for their data model,
             for these elements, CDATA must be handled differently by user agents.
             Markup and entities must be treated as raw text and passed to the application
             as is. The first occurrence of the character sequence "</" (end-tag open
             delimiter) is treated as terminating the end of the element's content. In
             valid documents, this would be the end tag for the element.
            */

            // discard everything until endtag is hit (except
            // if it occurs in a comment.

            // reset the stream mark to here, since we know that we sucessfully matched
            // a tag, and if we can't find the end tag, this is where we will want
            // to roll back to.
            saveState();
            pushed.setLength(0);
            return findEndTag();
        }
        return MATCH;
    }


    // find an end tag, but beware of comments...
    // <script><!-- </script> -->foo</script>
    // beware markup in script strings: </script>...document.write("</script>")foo</script>
    // TODO: do I need to worry about CDATA sections "<![CDATA["  ?

    int findEndTag() throws IOException {

        while ((numRead - lastMark) < safeReadAheadLimit) {
            int ch = next();
            if (ch == '<') {
                ch = next();
                // skip looking for end-tag in comments
                if (ch == '!') {
                    int ret = readBang(true);
                    if (ret == MATCH) continue;
                    // yikes... what now?  It wasn't a comment, but I can't get
                    // back to the state I was at.  Just continue from where I
                    // am I guess...
                    continue;
                }
                // did we match "</"
                if (ch != '/') {
                    push(ch);
                    continue;
                }
                int ret = readName(false);
                if (ret == MISMATCH) return MISMATCH;
                ch = nextSkipWS();
                if (ch != '>') return MISMATCH;
                return MATCH;
            } else if (ch == '\'' || ch == '"') {
                // read javascript string to avoid a false match.
                push(ch);
                int ret = readScriptString();
                // what to do about a non-match (non-terminated string?)
                // play it safe and index the rest of the data I guess...
                if (ret == MISMATCH) return MISMATCH;
            } else if (ch < 0) {
                return MISMATCH;
            }

        }
        return MISMATCH;
    }


    // read a string escaped by backslashes

    private int readScriptString() throws IOException {
        int quoteChar = next();
        if (quoteChar != '\'' && quoteChar != '"') return MISMATCH;

        while ((numRead - lastMark) < safeReadAheadLimit) {
            int ch = next();
            if (ch == quoteChar) return MATCH;
            else if (ch == '\\') {
                ch = next();
            } else if (ch < 0) {
                return MISMATCH;
            } else if (ch == '<') {
                eatSSI();
            }

        }
        return MISMATCH;
    }


    private int readName(boolean checkEscaped) throws IOException {
        StringBuilder builder = (checkEscaped && escapedTags != null) ? new StringBuilder() : null;
        int ch = next();
        if (builder != null) builder.append((char) ch);
        if (!isFirstIdChar(ch)) return MISMATCH;
        ch = next();
        if (builder != null) builder.append((char) ch);
        while (isIdChar(ch)) {
            ch = next();
            if (builder != null) builder.append((char) ch);
        }
        if (ch != -1) {
            push(ch);

        }
        //strip off the trailing >
        if (builder != null && escapedTags.contains(builder.substring(0, builder.length() - 1))) {
            return MISMATCH;
        }
        return MATCH;
    }

    /**
     * [10]    AttValue     ::=    '"' ([^<&"] | Reference)* '"'
     * |  "'" ([^<&'] | Reference)* "'"
     * <p/>
     * need to also handle unquoted attributes, and attributes w/o values:
     * <td id=msviGlobalToolbar height="22" nowrap align=left>
     * <p/>
     * *
     */

    // This reads attributes and attempts to handle any
    // embedded server side includes that would otherwise
    // mess up the quote handling.
    //  <a href="a/<!--#echo "path"-->">
    private int readAttr2() throws IOException {
        if ((numRead - lastMark < safeReadAheadLimit)) {
            int ch = next();
            if (!isFirstIdChar(ch)) return MISMATCH;
            ch = next();
            while (isIdChar(ch) && ((numRead - lastMark) < safeReadAheadLimit)) {
                ch = next();
            }
            if (isSpace(ch)) ch = nextSkipWS();

            // attributes may not have a value at all!
            // if (ch != '=') return MISMATCH;
            if (ch != '=') {
                push(ch);
                return MATCH;
            }

            int quoteChar = nextSkipWS();

            if (quoteChar == '"' || quoteChar == '\'') {
                while ((numRead - lastMark) < safeReadAheadLimit) {
                    ch = next();
                    if (ch < 0) return MISMATCH;
                    else if (ch == '<') {
                        eatSSI();
                    } else if (ch == quoteChar) {
                        return MATCH;
                        //} else if (ch=='<') {
                        //  return MISMATCH;
                    }

                }
            } else {
                // unquoted attribute
                while ((numRead - lastMark) < safeReadAheadLimit) {
                    ch = next();
                    if (ch < 0) return MISMATCH;
                    else if (isSpace(ch)) {
                        push(ch);
                        return MATCH;
                    } else if (ch == '>') {
                        push(ch);
                        return MATCH;
                    } else if (ch == '<') {
                        eatSSI();
                    }

                }
            }
        }
        return MISMATCH;
    }

    // skip past server side include

    private int eatSSI() throws IOException {
        // at this point, only a "<" was read.
        // on a mismatch, push back the last char so that if it was
        // a quote that closes the attribute, it will be re-read and matched.
        int ch = next();
        if (ch != '!') {
            push(ch);
            return MISMATCH;
        }
        ch = next();
        if (ch != '-') {
            push(ch);
            return MISMATCH;
        }
        ch = next();
        if (ch != '-') {
            push(ch);
            return MISMATCH;
        }
        ch = next();
        if (ch != '#') {
            push(ch);
            return MISMATCH;
        }

        push('#');
        push('-');
        push('-');
        return readComment(false);
    }

    private int readProcessingInstruction() throws IOException {
        // "<?" has already been read
        while ((numRead - lastMark) < safeReadAheadLimit) {
            int ch = next();
            if (ch == '?' && peek() == '>') {
                next();
                return MATCH;
            } else if (ch == -1) {
                return MISMATCH;
            }

        }
        return MISMATCH;
    }


    public int read() throws IOException {
        // TODO: Do we ever want to preserve CDATA sections?
        // where do we have to worry about them?
        // <![ CDATA [ unescaped markup ]]>
        if (numWhitespace > 0) {
            numEaten += numWhitespace;
            addOffCorrectMap(numReturned, numEaten);
            numWhitespace = 0;
        }
        numReturned++;
        //do not limit this one by the READAHEAD
        while (true) {
            int lastNumRead = numRead;
            int ch = next();

            switch (ch) {
                case '&':
                    saveState();
                    ch = readEntity();
                    if (ch >= 0) return ch;
                    if (ch == MISMATCH) {
                        restoreState();

                        return '&';
                    }
                    break;

                case '<':
                    saveState();
                    ch = next();
                    int ret = MISMATCH;
                    if (ch == '!') {
                        ret = readBang(false);
                    } else if (ch == '/') {
                        ret = readName(true);
                        if (ret == MATCH) {
                            ch = nextSkipWS();
                            ret = ch == '>' ? MATCH : MISMATCH;
                        }
                    } else if (isAlpha(ch)) {
                        push(ch);
                        ret = readTag();
                    } else if (ch == '?') {
                        ret = readProcessingInstruction();
                    }

                    // matched something to be discarded, so break
                    // from this case and continue in the loop
                    if (ret == MATCH) {
                        //break;//was
                        //return whitespace from
                        numWhitespace = (numRead - lastNumRead) - 1;//tack on the -1 since we are returning a space right now
                        return ' ';
                    }

                    // didn't match any HTML constructs, so roll back
                    // the stream state and just return '<'
                    restoreState();
                    return '<';

                default:
                    return ch;
            }

        }


    }

    public int read(char cbuf[], int off, int len) throws IOException {
        int i = 0;
        for (i = 0; i < len; i++) {
            int ch = read();
            if (ch == -1) break;
            cbuf[off++] = (char) ch;
        }
        if (i == 0) {
            if (len == 0) return 0;
            return -1;
        }
        return i;
    }

    public void close() throws IOException {
        input.close();
    }


    private static final HashMap<String, Character> entityTable;

    static {
        entityTable = new HashMap<String, Character>();
        // entityName and entityVal generated from the python script
        // included in comments at the end of this file.
        final String[] entityName = {"zwnj", "aring", "gt", "yen", "ograve", "Chi", "delta", "rang", "sup", "trade", "Ntilde", "xi", "upsih", "nbsp", "Atilde", "radic", "otimes", "aelig", "oelig", "equiv", "ni", "infin", "Psi", "auml", "cup", "Epsilon", "otilde", "lt", "Icirc", "Eacute", "Lambda", "sbquo", "Prime", "prime", "psi", "Kappa", "rsaquo", "Tau", "uacute", "ocirc", "lrm", "zwj", "cedil", "Alpha", "not", "amp", "AElig", "oslash", "acute", "lceil", "alefsym", "laquo", "shy", "loz", "ge", "Igrave", "nu", "Ograve", "lsaquo", "sube", "euro", "rarr", "sdot", "rdquo", "Yacute", "lfloor", "lArr", "Auml", "Dagger", "brvbar", "Otilde", "szlig", "clubs", "diams", "agrave", "Ocirc", "Iota", "Theta", "Pi", "zeta", "Scaron", "frac14", "egrave", "sub", "iexcl", "frac12", "ordf", "sum", "prop", "Uuml", "ntilde", "atilde", "asymp", "uml", "prod", "nsub", "reg", "rArr", "Oslash", "emsp", "THORN", "yuml", "aacute", "Mu", "hArr", "le", "thinsp", "dArr", "ecirc", "bdquo", "Sigma", "Aring", "tilde", "nabla", "mdash", "uarr", "times", "Ugrave", "Eta", "Agrave", "chi", "real", "circ", "eth", "rceil", "iuml", "gamma", "lambda", "harr", "Egrave", "frac34", "dagger", "divide", "Ouml", "image", "ndash", "hellip", "igrave", "Yuml", "ang", "alpha", "frasl", "ETH", "lowast", "Nu", "plusmn", "bull", "sup1", "sup2", "sup3", "Aacute", "cent", "oline", "Beta", "perp", "Delta", "there4", "pi", "iota", "empty", "euml", "notin", "iacute", "para", "epsilon", "weierp", "OElig", "uuml", "larr", "icirc", "Upsilon", "omicron", "upsilon", "copy", "Iuml", "Oacute", "Xi", "kappa", "ccedil", "Ucirc", "cap", "mu", "scaron", "lsquo", "isin", "Zeta", "minus", "deg", "and", "tau", "pound", "curren", "int", "ucirc", "rfloor", "ensp", "crarr", "ugrave", "exist", "cong", "theta", "oplus", "permil", "Acirc", "piv", "Euml", "Phi", "Iacute", "quot", "Uacute", "Omicron", "ne", "iquest", "eta", "rsquo", "yacute", "Rho", "darr", "Ecirc", "Omega", "acirc", "sim", "phi", "sigmaf", "macr", "thetasym", "Ccedil", "ordm", "uArr", "forall", "beta", "fnof", "rho", "micro", "eacute", "omega", "middot", "Gamma", "rlm", "lang", "spades", "supe", "thorn", "ouml", "or", "raquo", "part", "sect", "ldquo", "hearts", "sigma", "oacute"};
        final char[] entityVal = {8204, 229, 62, 165, 242, 935, 948, 9002, 8835, 8482, 209, 958, 978, 160, 195, 8730, 8855, 230, 339, 8801, 8715, 8734, 936, 228, 8746, 917, 245, 60, 206, 201, 923, 8218, 8243, 8242, 968, 922, 8250, 932, 250, 244, 8206, 8205, 184, 913, 172, 38, 198, 248, 180, 8968, 8501, 171, 173, 9674, 8805, 204, 957, 210, 8249, 8838, 8364, 8594, 8901, 8221, 221, 8970, 8656, 196, 8225, 166, 213, 223, 9827, 9830, 224, 212, 921, 920, 928, 950, 352, 188, 232, 8834, 161, 189, 170, 8721, 8733, 220, 241, 227, 8776, 168, 8719, 8836, 174, 8658, 216, 8195, 222, 255, 225, 924, 8660, 8804, 8201, 8659, 234, 8222, 931, 197, 732, 8711, 8212, 8593, 215, 217, 919, 192, 967, 8476, 710, 240, 8969, 239, 947, 955, 8596, 200, 190, 8224, 247, 214, 8465, 8211, 8230, 236, 376, 8736, 945, 8260, 208, 8727, 925, 177, 8226, 185, 178, 179, 193, 162, 8254, 914, 8869, 916, 8756, 960, 953, 8709, 235, 8713, 237, 182, 949, 8472, 338, 252, 8592, 238, 933, 959, 965, 169, 207, 211, 926, 954, 231, 219, 8745, 956, 353, 8216, 8712, 918, 8722, 176, 8743, 964, 163, 164, 8747, 251, 8971, 8194, 8629, 249, 8707, 8773, 952, 8853, 8240, 194, 982, 203, 934, 205, 34, 218, 927, 8800, 191, 951, 8217, 253, 929, 8595, 202, 937, 226, 8764, 966, 962, 175, 977, 199, 186, 8657, 8704, 946, 402, 961, 181, 233, 969, 183, 915, 8207, 9001, 9824, 8839, 254, 246, 8744, 187, 8706, 167, 8220, 9829, 963, 243};
        for (int i = 0; i < entityName.length; i++) {
            entityTable.put(entityName[i], new Character(entityVal[i]));
        }
        // special-case nbsp to a simple space instead of 0xa0
        entityTable.put("nbsp", new Character(' '));
    }

}

/********************* htmlentity.py **********************
 # a simple python script to generate an HTML entity table
 # from text taken from http://www.w3.org/TR/REC-html40/sgml/entities.html

 text="""
 24 Character entity references in HTML 4

 Contents

 1. Introduction to character entity references
 2. Character entity references for ISO 8859-1 characters
 1. The list of characters
 3. Character entity references for symbols, mathematical symbols, and Greek letters
 1. The list of characters
 4. Character entity references for markup-significant and internationalization characters
 1. The list of characters

 24.1 Introduction to character entity references
 A character entity reference is an SGML construct that references a character of the document character set.

 This version of HTML supports several sets of character entity references:

 * ISO 8859-1 (Latin-1) characters In accordance with section 14 of [RFC1866], the set of Latin-1 entities has been extended by this specification to cover the whole right part of ISO-8859-1 (all code positions with the high-order bit set), including the already commonly used &nbsp;, &copy; and &reg;. The names of the entities are taken from the appendices of SGML (defined in [ISO8879]).
 * symbols, mathematical symbols, and Greek letters. These characters may be represented by glyphs in the Adobe font "Symbol".
 * markup-significant and internationalization characters (e.g., for bidirectional text).

 The following sections present the complete lists of character entity references. Although, by convention, [ISO10646] the comments following each entry are usually written with uppercase letters, we have converted them to lowercase in this specification for reasons of readability.
 24.2 Character entity references for ISO 8859-1 characters

 The character entity references in this section produce characters whose numeric equivalents should already be supported by conforming HTML 2.0 user agents. Thus, the character entity reference &divide; is a more convenient form than &#247; for obtaining the division sign.

 To support these named entities, user agents need only recognize the entity names and convert them to characters that lie within the repertoire of [ISO88591].

 Character 65533 (FFFD hexadecimal) is the last valid character in UCS-2. 65534 (FFFE hexadecimal) is unassigned and reserved as the byte-swapped version of ZERO WIDTH NON-BREAKING SPACE for byte-order detection purposes. 65535 (FFFF hexadecimal) is unassigned.
 24.2.1 The list of characters

 <!-- Portions (c) International Organization for Standardization 1986
 Permission to copy in any form is granted for use with
 conforming SGML systems and applications as defined in
 ISO 8879, provided this notice is included in all copies.
 -->
 <!-- Character entity set. Typical invocation:
 <!ENTITY % HTMLlat1 PUBLIC
 "-//W3C//ENTITIES Latin 1//EN//HTML">
 %HTMLlat1;
 -->

 <!ENTITY nbsp   CDATA "&#160;" -- no-break space = non-breaking space,
 U+00A0 ISOnum -->
 <!ENTITY iexcl  CDATA "&#161;" -- inverted exclamation mark, U+00A1 ISOnum -->
 <!ENTITY cent   CDATA "&#162;" -- cent sign, U+00A2 ISOnum -->
 <!ENTITY pound  CDATA "&#163;" -- pound sign, U+00A3 ISOnum -->
 <!ENTITY curren CDATA "&#164;" -- currency sign, U+00A4 ISOnum -->
 <!ENTITY yen    CDATA "&#165;" -- yen sign = yuan sign, U+00A5 ISOnum -->
 <!ENTITY brvbar CDATA "&#166;" -- broken bar = broken vertical bar,
 U+00A6 ISOnum -->
 <!ENTITY sect   CDATA "&#167;" -- section sign, U+00A7 ISOnum -->
 <!ENTITY uml    CDATA "&#168;" -- diaeresis = spacing diaeresis,
 U+00A8 ISOdia -->
 <!ENTITY copy   CDATA "&#169;" -- copyright sign, U+00A9 ISOnum -->
 <!ENTITY ordf   CDATA "&#170;" -- feminine ordinal indicator, U+00AA ISOnum -->
 <!ENTITY laquo  CDATA "&#171;" -- left-pointing double angle quotation mark
 = left pointing guillemet, U+00AB ISOnum -->
 <!ENTITY not    CDATA "&#172;" -- not sign, U+00AC ISOnum -->
 <!ENTITY shy    CDATA "&#173;" -- soft hyphen = discretionary hyphen,
 U+00AD ISOnum -->
 <!ENTITY reg    CDATA "&#174;" -- registered sign = registered trade mark sign,
 U+00AE ISOnum -->
 <!ENTITY macr   CDATA "&#175;" -- macron = spacing macron = overline
 = APL overbar, U+00AF ISOdia -->
 <!ENTITY deg    CDATA "&#176;" -- degree sign, U+00B0 ISOnum -->
 <!ENTITY plusmn CDATA "&#177;" -- plus-minus sign = plus-or-minus sign,
 U+00B1 ISOnum -->
 <!ENTITY sup2   CDATA "&#178;" -- superscript two = superscript digit two
 = squared, U+00B2 ISOnum -->
 <!ENTITY sup3   CDATA "&#179;" -- superscript three = superscript digit three
 = cubed, U+00B3 ISOnum -->
 <!ENTITY acute  CDATA "&#180;" -- acute accent = spacing acute,
 U+00B4 ISOdia -->
 <!ENTITY micro  CDATA "&#181;" -- micro sign, U+00B5 ISOnum -->
 <!ENTITY para   CDATA "&#182;" -- pilcrow sign = paragraph sign,
 U+00B6 ISOnum -->
 <!ENTITY middot CDATA "&#183;" -- middle dot = Georgian comma
 = Greek middle dot, U+00B7 ISOnum -->
 <!ENTITY cedil  CDATA "&#184;" -- cedilla = spacing cedilla, U+00B8 ISOdia -->
 <!ENTITY sup1   CDATA "&#185;" -- superscript one = superscript digit one,
 U+00B9 ISOnum -->
 <!ENTITY ordm   CDATA "&#186;" -- masculine ordinal indicator,
 U+00BA ISOnum -->
 <!ENTITY raquo  CDATA "&#187;" -- right-pointing double angle quotation mark
 = right pointing guillemet, U+00BB ISOnum -->
 <!ENTITY frac14 CDATA "&#188;" -- vulgar fraction one quarter
 = fraction one quarter, U+00BC ISOnum -->
 <!ENTITY frac12 CDATA "&#189;" -- vulgar fraction one half
 = fraction one half, U+00BD ISOnum -->
 <!ENTITY frac34 CDATA "&#190;" -- vulgar fraction three quarters
 = fraction three quarters, U+00BE ISOnum -->
 <!ENTITY iquest CDATA "&#191;" -- inverted question mark
 = turned question mark, U+00BF ISOnum -->
 <!ENTITY Agrave CDATA "&#192;" -- latin capital letter A with grave
 = latin capital letter A grave,
 U+00C0 ISOlat1 -->
 <!ENTITY Aacute CDATA "&#193;" -- latin capital letter A with acute,
 U+00C1 ISOlat1 -->
 <!ENTITY Acirc  CDATA "&#194;" -- latin capital letter A with circumflex,
 U+00C2 ISOlat1 -->
 <!ENTITY Atilde CDATA "&#195;" -- latin capital letter A with tilde,
 U+00C3 ISOlat1 -->
 <!ENTITY Auml   CDATA "&#196;" -- latin capital letter A with diaeresis,
 U+00C4 ISOlat1 -->
 <!ENTITY Aring  CDATA "&#197;" -- latin capital letter A with ring above
 = latin capital letter A ring,
 U+00C5 ISOlat1 -->
 <!ENTITY AElig  CDATA "&#198;" -- latin capital letter AE
 = latin capital ligature AE,
 U+00C6 ISOlat1 -->
 <!ENTITY Ccedil CDATA "&#199;" -- latin capital letter C with cedilla,
 U+00C7 ISOlat1 -->
 <!ENTITY Egrave CDATA "&#200;" -- latin capital letter E with grave,
 U+00C8 ISOlat1 -->
 <!ENTITY Eacute CDATA "&#201;" -- latin capital letter E with acute,
 U+00C9 ISOlat1 -->
 <!ENTITY Ecirc  CDATA "&#202;" -- latin capital letter E with circumflex,
 U+00CA ISOlat1 -->
 <!ENTITY Euml   CDATA "&#203;" -- latin capital letter E with diaeresis,
 U+00CB ISOlat1 -->
 <!ENTITY Igrave CDATA "&#204;" -- latin capital letter I with grave,
 U+00CC ISOlat1 -->
 <!ENTITY Iacute CDATA "&#205;" -- latin capital letter I with acute,
 U+00CD ISOlat1 -->
 <!ENTITY Icirc  CDATA "&#206;" -- latin capital letter I with circumflex,
 U+00CE ISOlat1 -->
 <!ENTITY Iuml   CDATA "&#207;" -- latin capital letter I with diaeresis,
 U+00CF ISOlat1 -->
 <!ENTITY ETH    CDATA "&#208;" -- latin capital letter ETH, U+00D0 ISOlat1 -->
 <!ENTITY Ntilde CDATA "&#209;" -- latin capital letter N with tilde,
 U+00D1 ISOlat1 -->
 <!ENTITY Ograve CDATA "&#210;" -- latin capital letter O with grave,
 U+00D2 ISOlat1 -->
 <!ENTITY Oacute CDATA "&#211;" -- latin capital letter O with acute,
 U+00D3 ISOlat1 -->
 <!ENTITY Ocirc  CDATA "&#212;" -- latin capital letter O with circumflex,
 U+00D4 ISOlat1 -->
 <!ENTITY Otilde CDATA "&#213;" -- latin capital letter O with tilde,
 U+00D5 ISOlat1 -->
 <!ENTITY Ouml   CDATA "&#214;" -- latin capital letter O with diaeresis,
 U+00D6 ISOlat1 -->
 <!ENTITY times  CDATA "&#215;" -- multiplication sign, U+00D7 ISOnum -->
 <!ENTITY Oslash CDATA "&#216;" -- latin capital letter O with stroke
 = latin capital letter O slash,
 U+00D8 ISOlat1 -->
 <!ENTITY Ugrave CDATA "&#217;" -- latin capital letter U with grave,
 U+00D9 ISOlat1 -->
 <!ENTITY Uacute CDATA "&#218;" -- latin capital letter U with acute,
 U+00DA ISOlat1 -->
 <!ENTITY Ucirc  CDATA "&#219;" -- latin capital letter U with circumflex,
 U+00DB ISOlat1 -->
 <!ENTITY Uuml   CDATA "&#220;" -- latin capital letter U with diaeresis,
 U+00DC ISOlat1 -->
 <!ENTITY Yacute CDATA "&#221;" -- latin capital letter Y with acute,
 U+00DD ISOlat1 -->
 <!ENTITY THORN  CDATA "&#222;" -- latin capital letter THORN,
 U+00DE ISOlat1 -->
 <!ENTITY szlig  CDATA "&#223;" -- latin small letter sharp s = ess-zed,
 U+00DF ISOlat1 -->
 <!ENTITY agrave CDATA "&#224;" -- latin small letter a with grave
 = latin small letter a grave,
 U+00E0 ISOlat1 -->
 <!ENTITY aacute CDATA "&#225;" -- latin small letter a with acute,
 U+00E1 ISOlat1 -->
 <!ENTITY acirc  CDATA "&#226;" -- latin small letter a with circumflex,
 U+00E2 ISOlat1 -->
 <!ENTITY atilde CDATA "&#227;" -- latin small letter a with tilde,
 U+00E3 ISOlat1 -->
 <!ENTITY auml   CDATA "&#228;" -- latin small letter a with diaeresis,
 U+00E4 ISOlat1 -->
 <!ENTITY aring  CDATA "&#229;" -- latin small letter a with ring above
 = latin small letter a ring,
 U+00E5 ISOlat1 -->
 <!ENTITY aelig  CDATA "&#230;" -- latin small letter ae
 = latin small ligature ae, U+00E6 ISOlat1 -->
 <!ENTITY ccedil CDATA "&#231;" -- latin small letter c with cedilla,
 U+00E7 ISOlat1 -->
 <!ENTITY egrave CDATA "&#232;" -- latin small letter e with grave,
 U+00E8 ISOlat1 -->
 <!ENTITY eacute CDATA "&#233;" -- latin small letter e with acute,
 U+00E9 ISOlat1 -->
 <!ENTITY ecirc  CDATA "&#234;" -- latin small letter e with circumflex,
 U+00EA ISOlat1 -->
 <!ENTITY euml   CDATA "&#235;" -- latin small letter e with diaeresis,
 U+00EB ISOlat1 -->
 <!ENTITY igrave CDATA "&#236;" -- latin small letter i with grave,
 U+00EC ISOlat1 -->
 <!ENTITY iacute CDATA "&#237;" -- latin small letter i with acute,
 U+00ED ISOlat1 -->
 <!ENTITY icirc  CDATA "&#238;" -- latin small letter i with circumflex,
 U+00EE ISOlat1 -->
 <!ENTITY iuml   CDATA "&#239;" -- latin small letter i with diaeresis,
 U+00EF ISOlat1 -->
 <!ENTITY eth    CDATA "&#240;" -- latin small letter eth, U+00F0 ISOlat1 -->
 <!ENTITY ntilde CDATA "&#241;" -- latin small letter n with tilde,
 U+00F1 ISOlat1 -->
 <!ENTITY ograve CDATA "&#242;" -- latin small letter o with grave,
 U+00F2 ISOlat1 -->
 <!ENTITY oacute CDATA "&#243;" -- latin small letter o with acute,
 U+00F3 ISOlat1 -->
 <!ENTITY ocirc  CDATA "&#244;" -- latin small letter o with circumflex,
 U+00F4 ISOlat1 -->
 <!ENTITY otilde CDATA "&#245;" -- latin small letter o with tilde,
 U+00F5 ISOlat1 -->
 <!ENTITY ouml   CDATA "&#246;" -- latin small letter o with diaeresis,
 U+00F6 ISOlat1 -->
 <!ENTITY divide CDATA "&#247;" -- division sign, U+00F7 ISOnum -->
 <!ENTITY oslash CDATA "&#248;" -- latin small letter o with stroke,
 = latin small letter o slash,
 U+00F8 ISOlat1 -->
 <!ENTITY ugrave CDATA "&#249;" -- latin small letter u with grave,
 U+00F9 ISOlat1 -->
 <!ENTITY uacute CDATA "&#250;" -- latin small letter u with acute,
 U+00FA ISOlat1 -->
 <!ENTITY ucirc  CDATA "&#251;" -- latin small letter u with circumflex,
 U+00FB ISOlat1 -->
 <!ENTITY uuml   CDATA "&#252;" -- latin small letter u with diaeresis,
 U+00FC ISOlat1 -->
 <!ENTITY yacute CDATA "&#253;" -- latin small letter y with acute,
 U+00FD ISOlat1 -->
 <!ENTITY thorn  CDATA "&#254;" -- latin small letter thorn,
 U+00FE ISOlat1 -->
 <!ENTITY yuml   CDATA "&#255;" -- latin small letter y with diaeresis,
 U+00FF ISOlat1 -->

 24.3 Character entity references for symbols, mathematical symbols, and Greek letters

 The character entity references in this section produce characters that may be represented by glyphs in the widely available Adobe Symbol font, including Greek characters, various bracketing symbols, and a selection of mathematical operators such as gradient, product, and summation symbols.

 To support these entities, user agents may support full [ISO10646] or use other means. Display of glyphs for these characters may be obtained by being able to display the relevant [ISO10646] characters or by other means, such as internally mapping the listed entities, numeric character references, and characters to the appropriate position in some font that contains the requisite glyphs.

 When to use Greek entities. This entity set contains all the letters used in modern Greek. However, it does not include Greek punctuation, precomposed accented characters nor the non-spacing accents (tonos, dialytika) required to compose them. There are no archaic letters, Coptic-unique letters, or precomposed letters for Polytonic Greek. The entities defined here are not intended for the representation of modern Greek text and would not be an efficient representation; rather, they are intended for occasional Greek letters used in technical and mathematical works.
 24.3.1 The list of characters

 <!-- Mathematical, Greek and Symbolic characters for HTML -->

 <!-- Character entity set. Typical invocation:
 <!ENTITY % HTMLsymbol PUBLIC
 "-//W3C//ENTITIES Symbols//EN//HTML">
 %HTMLsymbol; -->

 <!-- Portions (c) International Organization for Standardization 1986:
 Permission to copy in any form is granted for use with
 conforming SGML systems and applications as defined in
 ISO 8879, provided this notice is included in all copies.
 -->

 <!-- Relevant ISO entity set is given unless names are newly introduced.
 New names (i.e., not in ISO 8879 list) do not clash with any
 existing ISO 8879 entity names. ISO 10646 character numbers
 are given for each character, in hex. CDATA values are decimal
 conversions of the ISO 10646 values and refer to the document
 character set. Names are ISO 10646 names.

 -->

 <!-- Latin Extended-B -->
 <!ENTITY fnof     CDATA "&#402;" -- latin small f with hook = function
 = florin, U+0192 ISOtech -->

 <!-- Greek -->
 <!ENTITY Alpha    CDATA "&#913;" -- greek capital letter alpha, U+0391 -->
 <!ENTITY Beta     CDATA "&#914;" -- greek capital letter beta, U+0392 -->
 <!ENTITY Gamma    CDATA "&#915;" -- greek capital letter gamma,
 U+0393 ISOgrk3 -->
 <!ENTITY Delta    CDATA "&#916;" -- greek capital letter delta,
 U+0394 ISOgrk3 -->
 <!ENTITY Epsilon  CDATA "&#917;" -- greek capital letter epsilon, U+0395 -->
 <!ENTITY Zeta     CDATA "&#918;" -- greek capital letter zeta, U+0396 -->
 <!ENTITY Eta      CDATA "&#919;" -- greek capital letter eta, U+0397 -->
 <!ENTITY Theta    CDATA "&#920;" -- greek capital letter theta,
 U+0398 ISOgrk3 -->
 <!ENTITY Iota     CDATA "&#921;" -- greek capital letter iota, U+0399 -->
 <!ENTITY Kappa    CDATA "&#922;" -- greek capital letter kappa, U+039A -->
 <!ENTITY Lambda   CDATA "&#923;" -- greek capital letter lambda,
 U+039B ISOgrk3 -->
 <!ENTITY Mu       CDATA "&#924;" -- greek capital letter mu, U+039C -->
 <!ENTITY Nu       CDATA "&#925;" -- greek capital letter nu, U+039D -->
 <!ENTITY Xi       CDATA "&#926;" -- greek capital letter xi, U+039E ISOgrk3 -->
 <!ENTITY Omicron  CDATA "&#927;" -- greek capital letter omicron, U+039F -->
 <!ENTITY Pi       CDATA "&#928;" -- greek capital letter pi, U+03A0 ISOgrk3 -->
 <!ENTITY Rho      CDATA "&#929;" -- greek capital letter rho, U+03A1 -->
 <!-- there is no Sigmaf, and no U+03A2 character either -->
 <!ENTITY Sigma    CDATA "&#931;" -- greek capital letter sigma,
 U+03A3 ISOgrk3 -->
 <!ENTITY Tau      CDATA "&#932;" -- greek capital letter tau, U+03A4 -->
 <!ENTITY Upsilon  CDATA "&#933;" -- greek capital letter upsilon,
 U+03A5 ISOgrk3 -->
 <!ENTITY Phi      CDATA "&#934;" -- greek capital letter phi,
 U+03A6 ISOgrk3 -->
 <!ENTITY Chi      CDATA "&#935;" -- greek capital letter chi, U+03A7 -->
 <!ENTITY Psi      CDATA "&#936;" -- greek capital letter psi,
 U+03A8 ISOgrk3 -->
 <!ENTITY Omega    CDATA "&#937;" -- greek capital letter omega,
 U+03A9 ISOgrk3 -->

 <!ENTITY alpha    CDATA "&#945;" -- greek small letter alpha,
 U+03B1 ISOgrk3 -->
 <!ENTITY beta     CDATA "&#946;" -- greek small letter beta, U+03B2 ISOgrk3 -->
 <!ENTITY gamma    CDATA "&#947;" -- greek small letter gamma,
 U+03B3 ISOgrk3 -->
 <!ENTITY delta    CDATA "&#948;" -- greek small letter delta,
 U+03B4 ISOgrk3 -->
 <!ENTITY epsilon  CDATA "&#949;" -- greek small letter epsilon,
 U+03B5 ISOgrk3 -->
 <!ENTITY zeta     CDATA "&#950;" -- greek small letter zeta, U+03B6 ISOgrk3 -->
 <!ENTITY eta      CDATA "&#951;" -- greek small letter eta, U+03B7 ISOgrk3 -->
 <!ENTITY theta    CDATA "&#952;" -- greek small letter theta,
 U+03B8 ISOgrk3 -->
 <!ENTITY iota     CDATA "&#953;" -- greek small letter iota, U+03B9 ISOgrk3 -->
 <!ENTITY kappa    CDATA "&#954;" -- greek small letter kappa,
 U+03BA ISOgrk3 -->
 <!ENTITY lambda   CDATA "&#955;" -- greek small letter lambda,
 U+03BB ISOgrk3 -->
 <!ENTITY mu       CDATA "&#956;" -- greek small letter mu, U+03BC ISOgrk3 -->
 <!ENTITY nu       CDATA "&#957;" -- greek small letter nu, U+03BD ISOgrk3 -->
 <!ENTITY xi       CDATA "&#958;" -- greek small letter xi, U+03BE ISOgrk3 -->
 <!ENTITY omicron  CDATA "&#959;" -- greek small letter omicron, U+03BF NEW -->
 <!ENTITY pi       CDATA "&#960;" -- greek small letter pi, U+03C0 ISOgrk3 -->
 <!ENTITY rho      CDATA "&#961;" -- greek small letter rho, U+03C1 ISOgrk3 -->
 <!ENTITY sigmaf   CDATA "&#962;" -- greek small letter final sigma,
 U+03C2 ISOgrk3 -->
 <!ENTITY sigma    CDATA "&#963;" -- greek small letter sigma,
 U+03C3 ISOgrk3 -->
 <!ENTITY tau      CDATA "&#964;" -- greek small letter tau, U+03C4 ISOgrk3 -->
 <!ENTITY upsilon  CDATA "&#965;" -- greek small letter upsilon,
 U+03C5 ISOgrk3 -->
 <!ENTITY phi      CDATA "&#966;" -- greek small letter phi, U+03C6 ISOgrk3 -->
 <!ENTITY chi      CDATA "&#967;" -- greek small letter chi, U+03C7 ISOgrk3 -->
 <!ENTITY psi      CDATA "&#968;" -- greek small letter psi, U+03C8 ISOgrk3 -->
 <!ENTITY omega    CDATA "&#969;" -- greek small letter omega,
 U+03C9 ISOgrk3 -->
 <!ENTITY thetasym CDATA "&#977;" -- greek small letter theta symbol,
 U+03D1 NEW -->
 <!ENTITY upsih    CDATA "&#978;" -- greek upsilon with hook symbol,
 U+03D2 NEW -->
 <!ENTITY piv      CDATA "&#982;" -- greek pi symbol, U+03D6 ISOgrk3 -->

 <!-- General Punctuation -->
 <!ENTITY bull     CDATA "&#8226;" -- bullet = black small circle,
 U+2022 ISOpub  -->
 <!-- bullet is NOT the same as bullet operator, U+2219 -->
 <!ENTITY hellip   CDATA "&#8230;" -- horizontal ellipsis = three dot leader,
 U+2026 ISOpub  -->
 <!ENTITY prime    CDATA "&#8242;" -- prime = minutes = feet, U+2032 ISOtech -->
 <!ENTITY Prime    CDATA "&#8243;" -- double prime = seconds = inches,
 U+2033 ISOtech -->
 <!ENTITY oline    CDATA "&#8254;" -- overline = spacing overscore,
 U+203E NEW -->
 <!ENTITY frasl    CDATA "&#8260;" -- fraction slash, U+2044 NEW -->

 <!-- Letterlike Symbols -->
 <!ENTITY weierp   CDATA "&#8472;" -- script capital P = power set
 = Weierstrass p, U+2118 ISOamso -->
 <!ENTITY image    CDATA "&#8465;" -- blackletter capital I = imaginary part,
 U+2111 ISOamso -->
 <!ENTITY real     CDATA "&#8476;" -- blackletter capital R = real part symbol,
 U+211C ISOamso -->
 <!ENTITY trade    CDATA "&#8482;" -- trade mark sign, U+2122 ISOnum -->
 <!ENTITY alefsym  CDATA "&#8501;" -- alef symbol = first transfinite cardinal,
 U+2135 NEW -->
 <!-- alef symbol is NOT the same as hebrew letter alef,
 U+05D0 although the same glyph could be used to depict both characters -->

 <!-- Arrows -->
 <!ENTITY larr     CDATA "&#8592;" -- leftwards arrow, U+2190 ISOnum -->
 <!ENTITY uarr     CDATA "&#8593;" -- upwards arrow, U+2191 ISOnum-->
 <!ENTITY rarr     CDATA "&#8594;" -- rightwards arrow, U+2192 ISOnum -->
 <!ENTITY darr     CDATA "&#8595;" -- downwards arrow, U+2193 ISOnum -->
 <!ENTITY harr     CDATA "&#8596;" -- left right arrow, U+2194 ISOamsa -->
 <!ENTITY crarr    CDATA "&#8629;" -- downwards arrow with corner leftwards
 = carriage return, U+21B5 NEW -->
 <!ENTITY lArr     CDATA "&#8656;" -- leftwards double arrow, U+21D0 ISOtech -->
 <!-- ISO 10646 does not say that lArr is the same as the 'is implied by' arrow
 but also does not have any other character for that function. So ? lArr can
 be used for 'is implied by' as ISOtech suggests -->
 <!ENTITY uArr     CDATA "&#8657;" -- upwards double arrow, U+21D1 ISOamsa -->
 <!ENTITY rArr     CDATA "&#8658;" -- rightwards double arrow,
 U+21D2 ISOtech -->
 <!-- ISO 10646 does not say this is the 'implies' character but does not have
 another character with this function so ?
 rArr can be used for 'implies' as ISOtech suggests -->
 <!ENTITY dArr     CDATA "&#8659;" -- downwards double arrow, U+21D3 ISOamsa -->
 <!ENTITY hArr     CDATA "&#8660;" -- left right double arrow,
 U+21D4 ISOamsa -->

 <!-- Mathematical Operators -->
 <!ENTITY forall   CDATA "&#8704;" -- for all, U+2200 ISOtech -->
 <!ENTITY part     CDATA "&#8706;" -- partial differential, U+2202 ISOtech  -->
 <!ENTITY exist    CDATA "&#8707;" -- there exists, U+2203 ISOtech -->
 <!ENTITY empty    CDATA "&#8709;" -- empty set = null set = diameter,
 U+2205 ISOamso -->
 <!ENTITY nabla    CDATA "&#8711;" -- nabla = backward difference,
 U+2207 ISOtech -->
 <!ENTITY isin     CDATA "&#8712;" -- element of, U+2208 ISOtech -->
 <!ENTITY notin    CDATA "&#8713;" -- not an element of, U+2209 ISOtech -->
 <!ENTITY ni       CDATA "&#8715;" -- contains as member, U+220B ISOtech -->
 <!-- should there be a more memorable name than 'ni'? -->
 <!ENTITY prod     CDATA "&#8719;" -- n-ary product = product sign,
 U+220F ISOamsb -->
 <!-- prod is NOT the same character as U+03A0 'greek capital letter pi' though
 the same glyph might be used for both -->
 <!ENTITY sum      CDATA "&#8721;" -- n-ary sumation, U+2211 ISOamsb -->
 <!-- sum is NOT the same character as U+03A3 'greek capital letter sigma'
 though the same glyph might be used for both -->
 <!ENTITY minus    CDATA "&#8722;" -- minus sign, U+2212 ISOtech -->
 <!ENTITY lowast   CDATA "&#8727;" -- asterisk operator, U+2217 ISOtech -->
 <!ENTITY radic    CDATA "&#8730;" -- square root = radical sign,
 U+221A ISOtech -->
 <!ENTITY prop     CDATA "&#8733;" -- proportional to, U+221D ISOtech -->
 <!ENTITY infin    CDATA "&#8734;" -- infinity, U+221E ISOtech -->
 <!ENTITY ang      CDATA "&#8736;" -- angle, U+2220 ISOamso -->
 <!ENTITY and      CDATA "&#8743;" -- logical and = wedge, U+2227 ISOtech -->
 <!ENTITY or       CDATA "&#8744;" -- logical or = vee, U+2228 ISOtech -->
 <!ENTITY cap      CDATA "&#8745;" -- intersection = cap, U+2229 ISOtech -->
 <!ENTITY cup      CDATA "&#8746;" -- union = cup, U+222A ISOtech -->
 <!ENTITY int      CDATA "&#8747;" -- integral, U+222B ISOtech -->
 <!ENTITY there4   CDATA "&#8756;" -- therefore, U+2234 ISOtech -->
 <!ENTITY sim      CDATA "&#8764;" -- tilde operator = varies with = similar to,
 U+223C ISOtech -->
 <!-- tilde operator is NOT the same character as the tilde, U+007E,
 although the same glyph might be used to represent both  -->
 <!ENTITY cong     CDATA "&#8773;" -- approximately equal to, U+2245 ISOtech -->
 <!ENTITY asymp    CDATA "&#8776;" -- almost equal to = asymptotic to,
 U+2248 ISOamsr -->
 <!ENTITY ne       CDATA "&#8800;" -- not equal to, U+2260 ISOtech -->
 <!ENTITY equiv    CDATA "&#8801;" -- identical to, U+2261 ISOtech -->
 <!ENTITY le       CDATA "&#8804;" -- less-than or equal to, U+2264 ISOtech -->
 <!ENTITY ge       CDATA "&#8805;" -- greater-than or equal to,
 U+2265 ISOtech -->
 <!ENTITY sub      CDATA "&#8834;" -- subset of, U+2282 ISOtech -->
 <!ENTITY sup      CDATA "&#8835;" -- superset of, U+2283 ISOtech -->
 <!-- note that nsup, 'not a superset of, U+2283' is not covered by the Symbol
 font encoding and is not included. Should it be, for symmetry?
 It is in ISOamsn  -->
 <!ENTITY nsub     CDATA "&#8836;" -- not a subset of, U+2284 ISOamsn -->
 <!ENTITY sube     CDATA "&#8838;" -- subset of or equal to, U+2286 ISOtech -->
 <!ENTITY supe     CDATA "&#8839;" -- superset of or equal to,
 U+2287 ISOtech -->
 <!ENTITY oplus    CDATA "&#8853;" -- circled plus = direct sum,
 U+2295 ISOamsb -->
 <!ENTITY otimes   CDATA "&#8855;" -- circled times = vector product,
 U+2297 ISOamsb -->
 <!ENTITY perp     CDATA "&#8869;" -- up tack = orthogonal to = perpendicular,
 U+22A5 ISOtech -->
 <!ENTITY sdot     CDATA "&#8901;" -- dot operator, U+22C5 ISOamsb -->
 <!-- dot operator is NOT the same character as U+00B7 middle dot -->

 <!-- Miscellaneous Technical -->
 <!ENTITY lceil    CDATA "&#8968;" -- left ceiling = apl upstile,
 U+2308 ISOamsc  -->
 <!ENTITY rceil    CDATA "&#8969;" -- right ceiling, U+2309 ISOamsc  -->
 <!ENTITY lfloor   CDATA "&#8970;" -- left floor = apl downstile,
 U+230A ISOamsc  -->
 <!ENTITY rfloor   CDATA "&#8971;" -- right floor, U+230B ISOamsc  -->
 <!ENTITY lang     CDATA "&#9001;" -- left-pointing angle bracket = bra,
 U+2329 ISOtech -->
 <!-- lang is NOT the same character as U+003C 'less than'
 or U+2039 'single left-pointing angle quotation mark' -->
 <!ENTITY rang     CDATA "&#9002;" -- right-pointing angle bracket = ket,
 U+232A ISOtech -->
 <!-- rang is NOT the same character as U+003E 'greater than'
 or U+203A 'single right-pointing angle quotation mark' -->

 <!-- Geometric Shapes -->
 <!ENTITY loz      CDATA "&#9674;" -- lozenge, U+25CA ISOpub -->

 <!-- Miscellaneous Symbols -->
 <!ENTITY spades   CDATA "&#9824;" -- black spade suit, U+2660 ISOpub -->
 <!-- black here seems to mean filled as opposed to hollow -->
 <!ENTITY clubs    CDATA "&#9827;" -- black club suit = shamrock,
 U+2663 ISOpub -->
 <!ENTITY hearts   CDATA "&#9829;" -- black heart suit = valentine,
 U+2665 ISOpub -->
 <!ENTITY diams    CDATA "&#9830;" -- black diamond suit, U+2666 ISOpub -->

 24.4 Character entity references for markup-significant and internationalization characters

 The character entity references in this section are for escaping markup-significant characters (these are the same as those in HTML 2.0 and 3.2), for denoting spaces and dashes. Other characters in this section apply to internationalization issues such as the disambiguation of bidirectional text (see the section on bidirectional text for details).

 Entities have also been added for the remaining characters occurring in CP-1252 which do not occur in the HTMLlat1 or HTMLsymbol entity sets. These all occur in the 128 to 159 range within the CP-1252 charset. These entities permit the characters to be denoted in a platform-independent manner.

 To support these entities, user agents may support full [ISO10646] or use other means. Display of glyphs for these characters may be obtained by being able to display the relevant [ISO10646] characters or by other means, such as internally mapping the listed entities, numeric character references, and characters to the appropriate position in some font that contains the requisite glyphs.
 24.4.1 The list of characters

 <!-- Special characters for HTML -->

 <!-- Character entity set. Typical invocation:
 <!ENTITY % HTMLspecial PUBLIC
 "-//W3C//ENTITIES Special//EN//HTML">
 %HTMLspecial; -->

 <!-- Portions (c) International Organization for Standardization 1986:
 Permission to copy in any form is granted for use with
 conforming SGML systems and applications as defined in
 ISO 8879, provided this notice is included in all copies.
 -->

 <!-- Relevant ISO entity set is given unless names are newly introduced.
 New names (i.e., not in ISO 8879 list) do not clash with any
 existing ISO 8879 entity names. ISO 10646 character numbers
 are given for each character, in hex. CDATA values are decimal
 conversions of the ISO 10646 values and refer to the document
 character set. Names are ISO 10646 names.

 -->

 <!-- C0 Controls and Basic Latin -->
 <!ENTITY quot    CDATA "&#34;"   -- quotation mark = APL quote,
 U+0022 ISOnum -->
 <!ENTITY amp     CDATA "&#38;"   -- ampersand, U+0026 ISOnum -->
 <!ENTITY lt      CDATA "&#60;"   -- less-than sign, U+003C ISOnum -->
 <!ENTITY gt      CDATA "&#62;"   -- greater-than sign, U+003E ISOnum -->

 <!-- Latin Extended-A -->
 <!ENTITY OElig   CDATA "&#338;"  -- latin capital ligature OE,
 U+0152 ISOlat2 -->
 <!ENTITY oelig   CDATA "&#339;"  -- latin small ligature oe, U+0153 ISOlat2 -->
 <!-- ligature is a misnomer, this is a separate character in some languages -->
 <!ENTITY Scaron  CDATA "&#352;"  -- latin capital letter S with caron,
 U+0160 ISOlat2 -->
 <!ENTITY scaron  CDATA "&#353;"  -- latin small letter s with caron,
 U+0161 ISOlat2 -->
 <!ENTITY Yuml    CDATA "&#376;"  -- latin capital letter Y with diaeresis,
 U+0178 ISOlat2 -->

 <!-- Spacing Modifier Letters -->
 <!ENTITY circ    CDATA "&#710;"  -- modifier letter circumflex accent,
 U+02C6 ISOpub -->
 <!ENTITY tilde   CDATA "&#732;"  -- small tilde, U+02DC ISOdia -->

 <!-- General Punctuation -->
 <!ENTITY ensp    CDATA "&#8194;" -- en space, U+2002 ISOpub -->
 <!ENTITY emsp    CDATA "&#8195;" -- em space, U+2003 ISOpub -->
 <!ENTITY thinsp  CDATA "&#8201;" -- thin space, U+2009 ISOpub -->
 <!ENTITY zwnj    CDATA "&#8204;" -- zero width non-joiner,
 U+200C NEW RFC 2070 -->
 <!ENTITY zwj     CDATA "&#8205;" -- zero width joiner, U+200D NEW RFC 2070 -->
 <!ENTITY lrm     CDATA "&#8206;" -- left-to-right mark, U+200E NEW RFC 2070 -->
 <!ENTITY rlm     CDATA "&#8207;" -- right-to-left mark, U+200F NEW RFC 2070 -->
 <!ENTITY ndash   CDATA "&#8211;" -- en dash, U+2013 ISOpub -->
 <!ENTITY mdash   CDATA "&#8212;" -- em dash, U+2014 ISOpub -->
 <!ENTITY lsquo   CDATA "&#8216;" -- left single quotation mark,
 U+2018 ISOnum -->
 <!ENTITY rsquo   CDATA "&#8217;" -- right single quotation mark,
 U+2019 ISOnum -->
 <!ENTITY sbquo   CDATA "&#8218;" -- single low-9 quotation mark, U+201A NEW -->
 <!ENTITY ldquo   CDATA "&#8220;" -- left double quotation mark,
 U+201C ISOnum -->
 <!ENTITY rdquo   CDATA "&#8221;" -- right double quotation mark,
 U+201D ISOnum -->
 <!ENTITY bdquo   CDATA "&#8222;" -- double low-9 quotation mark, U+201E NEW -->
 <!ENTITY dagger  CDATA "&#8224;" -- dagger, U+2020 ISOpub -->
 <!ENTITY Dagger  CDATA "&#8225;" -- double dagger, U+2021 ISOpub -->
 <!ENTITY permil  CDATA "&#8240;" -- per mille sign, U+2030 ISOtech -->
 <!ENTITY lsaquo  CDATA "&#8249;" -- single left-pointing angle quotation mark,
 U+2039 ISO proposed -->
 <!-- lsaquo is proposed but not yet ISO standardized -->
 <!ENTITY rsaquo  CDATA "&#8250;" -- single right-pointing angle quotation mark,
 U+203A ISO proposed -->
 <!-- rsaquo is proposed but not yet ISO standardized -->
 <!ENTITY euro   CDATA "&#8364;"  -- euro sign, U+20AC NEW -->
 """

 codes={}
 for line in text.split('\n'):
 parts = line.split()
 if len(parts)<3 or parts[0]!='<!ENTITY' or parts[2]!='CDATA': continue
 codes[parts[1]] = parts[3].strip('&#";')

 print 'entityName={', ','.join([ '"'+key+'"' for key in codes]), '};'
 print 'entityVal={', ','.join([ str(codes[key]) for key in codes]), '};'


 ********************** end htmlentity.py ********************/

