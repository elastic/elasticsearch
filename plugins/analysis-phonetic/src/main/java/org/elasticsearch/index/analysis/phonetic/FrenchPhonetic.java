package org.elasticsearch.index.analysis.phonetic;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.StringEncoder;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * <p>French phonetic plugin has been developped to increase number of matching words based on the way they are pronounced in French.</p>
 * </p>All current latin phonetic plugins matched too much french words as equal.</p>
 * </p>Not all phonemes have been coded because it would have been too restrictive.</p>
 *
 * <pre>
 *     1 for in sound
 *     2 for &eacute; sound
 *     3 for an sound
 *     4 for on sound
 *     8 for oeu/eu sound
 * </pre>
 *
 * @see <a href="https://fr.wikipedia.org/wiki/Prononciation_du_fran%C3%A7ais#Prononciation_des_graph.C3.A8mes"/> French prononciation on Wikipedia</a>
 *
 * @author Harold Capitaine
 * @author Galeries Lafayette
 */
public class FrenchPhonetic implements StringEncoder {

    private static final List<Character> VOWELS = Arrays.asList('A', 'E', '2', 'I', 'O', 'U', 'Y');

    private static final List<Character> MUTED_ENDED_CONSONANT = Arrays.asList('C', 'D', 'H', 'G', 'P', 'S', 'T', 'X', 'Z');

    private static final List<Character> DOUBLE_CONSONANT = Arrays.asList('C', 'P', 'R', 'T', 'Z', 'M', 'G', 'L');

    @Override
    public String encode(String s) throws EncoderException {
        String cleanedString = clean(s);

        if (cleanedString == null || cleanedString.length() == 0) {
            return cleanedString;
        }


        String result = operatePhonetic("", charAt(cleanedString, 0), substring(cleanedString, 1, cleanedString.length()));

        return result;
    }

    private String operatePhonetic(String acc, Character c, String tail) {

        if (c == null) {
            return acc;
        }

        if (tail == null || tail.isEmpty()) {

            //Trailing muted consonant
            if (MUTED_ENDED_CONSONANT.contains(c)) {
                if (c != 'X') {
                    return operatePhonetic(
                            substring(acc, 0, acc.length() - 1),
                            charAt(acc, acc.length() - 1),
                            ""
                    );
                } else {

                    //1X must be pronounced
                    if (!Character.valueOf('1').equals(charAt(acc, acc.length() - 1)) && Character.valueOf('U').equals(charAt(acc, acc.length() - 1))) {
                        return operatePhonetic(
                                substring(acc, 0, acc.length() - 1),
                                charAt(acc, acc.length() - 1),
                                ""
                        );
                    }
                }
            }

        } else {

            //Muted starting H with vowels
            if ((acc.isEmpty() || ( !Character.valueOf('C').equals(charAt(acc, acc.length() - 1) ) && !Character.valueOf('S').equals(charAt(acc, acc.length() - 1) ) )) && c == 'H') {
                if (VOWELS.contains(tail.charAt(0))) {
                    return operatePhonetic(acc, tail.charAt(0), substring(tail, 1, tail.length()));
                }
            }

            if (!acc.isEmpty() && VOWELS.contains(acc.charAt(acc.length() - 1)) && c == 'H') {
                return operatePhonetic(acc, tail.charAt(0), substring(tail, 1, tail.length()));
            }

            //C as S
            if (c == 'C' && (tail.charAt(0) == 'E' || tail.charAt(0) == 'I' || tail.charAt(0) == 'Y')) {
                return operatePhonetic(acc + 'S', tail.charAt(0), substring(tail, 1, tail.length()));
            } else {
                //C as K
                if (c == 'C' && tail.charAt(0) != 'E' && tail.charAt(0) != 'I' && tail.charAt(0) != 'Y' && tail.charAt(0) != 'H') {
                    if (!acc.isEmpty() && charAt(acc, acc.length() - 1) == 'K' || charAt(tail, 0) == 'K') {
                        return operatePhonetic(acc, tail.charAt(0), substring(tail, 1, tail.length()));
                    } else {
                        return operatePhonetic(acc + 'K', tail.charAt(0), substring(tail, 1, tail.length()));
                    }
                }
            }

            if (c == 'O' && tail.length() >= 2 && charAt(tail, 0) == 'E' && charAt(tail, 1) == 'U') {
                return operatePhonetic(acc + "8", charAt(tail, 2), substring(tail, 3, tail.length()));
            }

            if (c == 'E' && charAt(tail, 0) == 'U') {
                return operatePhonetic(acc + "8", charAt(tail, 1), substring(tail, 2, tail.length()));
            }

            //G as J
            if (c == 'G' && (tail.charAt(0) == 'E' || tail.charAt(0) == 'I' || tail.charAt(0) == 'Y')) {
                return operatePhonetic(acc + 'J', tail.charAt(0), substring(tail, 1, tail.length()));
            }

            //S as Z
            if (c == 'S' && !acc.isEmpty() && VOWELS.contains(acc.charAt(acc.length() - 1)) && VOWELS.contains(tail.charAt(0))) {
                return operatePhonetic(acc + 'Z', tail.charAt(0), substring(tail, 1, tail.length()));
            }

            //W as V
            if (c == 'W') {
                return operatePhonetic(acc + 'V', tail.charAt(0), substring(tail, 1, tail.length()));
            }

            //remove double consonant ex
            if (isDoubleConsonnant(c, tail)) {
                return operatePhonetic(acc, c, substring(tail, 1, tail.length()));
            }

            //Q, QU as K
            if (c == 'Q' && tail.charAt(0) == 'U') {
                return operatePhonetic(acc + 'K', charAt(tail, 1), substring(tail, 2, tail.length()));
            }

            //PH as F
            if (c == 'P' && tail.charAt(0) == 'H') {
                return operatePhonetic(acc + 'F', charAt(tail, 1), substring(tail, 2, tail.length()));
            }


            String replacedThreeLettersINSound = replaceThreeLettersINSound(acc, c, tail, 'A', 'E');
            if (replacedThreeLettersINSound != null) {
                return replacedThreeLettersINSound;
            }

            //EAU as O
            if (c == 'E' && tail.length() >= 2 && tail.charAt(0) == 'A' && tail.charAt(1) == 'U') {
                return operatePhonetic(acc + "O", charAt(tail, 2), substring(tail, 3, tail.length()));
            }

            String replacedTwoLettersSounds = replaceTwoLettersSounds(acc, c, tail);
            if (replacedTwoLettersSounds != null) {
                return replacedTwoLettersSounds;
            }


            if (c == 'T' && tail.length() >= 3 && VOWELS.contains(charAt(acc, acc.length() - 1)) && "ION".equals(substring(tail, 0, 3))) {
                return operatePhonetic(acc + "S", charAt(tail, 0), substring(tail, 1, tail.length()));
            }


        }

        if (c == 'E') {
            String nextAcc = operatePhonetic("2", charAt(tail, 0), substring(tail, 1, tail.length()));
            if ("2".equals(nextAcc)) {
                return acc;
            } else {
                nextAcc = nextAcc.substring(1, nextAcc.length());
            }
            return acc + "2" + nextAcc;
        }

        //Y as I
        if (c == 'Y') {
            return operatePhonetic(acc + 'I', charAt(tail, 0), substring(tail, 1, tail.length()));
        }

        return operatePhonetic(acc + c, charAt(tail, 0), substring(tail, 1, tail.length()));
    }

    private boolean isDoubleConsonnant(Character c, String tail) {
        Character character = charAt(tail, 0);
        return c != null && character != null && DOUBLE_CONSONANT.contains(c.charValue()) && character.charValue() == c.charValue();
    }

    private String replaceTwoLettersSounds(String acc, char c, String tail) {

        //Trailing ER, ET as 2
        if (c == 'E' && tail.length() >= 1 && (tail.charAt(0) == 'R' || tail.charAt(0) == 'T') && !isDoubleConsonnant(tail.charAt(0), substring(tail, 1, tail.length()))) {
            String encodedTail = operatePhonetic("", charAt(tail, 1), substring(tail, 2, tail.length()));
            if (encodedTail.isEmpty()) {
                return acc + "2";
            } else {
                return acc + "2" + tail.charAt(0) + encodedTail;
            }


        }

        // AU as O
        if (c == 'A' && tail.charAt(0) == 'U') {
            return operatePhonetic(acc + 'O', charAt(tail, 1), substring(tail, 2, tail.length()));
        }


        String replacedAISound = replaceAISounds(acc, c, tail, 'A', 'E');
        if (replacedAISound != null) {
            return replacedAISound;
        }

        String replacedONSound = replaceONOrINOrANSound(acc, c, tail, "4", 'O');
        if (replacedONSound != null) {
            return replacedONSound;
        }


        String replacedINSound = replaceONOrINOrANSound(acc, c, tail, "1", 'Y', 'I', 'U');
        if (replacedINSound != null) {
            return replacedINSound;
        }

        String replacedANSound = replaceONOrINOrANSound(acc, c, tail, "3", 'A', 'E');
        if (replacedANSound != null) {
            return replacedANSound;
        }

        return null;
    }

    private String replaceAISounds(String acc, char c, String tail, Character... firstLetters) {
        if (Arrays.asList(firstLetters).contains(c) && (tail.charAt(0) == 'I' || tail.charAt(0) == 'Y')) {
            acc += "2";
            if (tail.charAt(0) == 'Y' && tail.length() != 1 && (c != 'E' || c == 'E' && VOWELS.contains(tail.charAt(1)))) {
                acc += "I";
            }
            return operatePhonetic(acc, charAt(tail, 1), substring(tail, 2, tail.length()));
        }
        return null;
    }

    private String replaceThreeLettersINSound(String acc, char c, String tail, Character... firstLetters) {
        if (Arrays.asList(firstLetters).contains(c) && tail.length() >= 2 &&
                tail.charAt(0) == 'I' && (tail.charAt(1) == 'N' || tail.charAt(1) == 'M')) {
            if (tail.length() >= 3 && (tail.charAt(2) == 'M' || tail.charAt(2) == 'N' || tail.charAt(2) == 'H' || VOWELS.contains(tail.charAt(2)))) {
                return replaceTwoLettersSounds(acc, c, tail);
            }
            return operatePhonetic(acc + "1", charAt(tail, 2), substring(tail, 3, tail.length()));
        }
        return null;
    }

    private String replaceONOrINOrANSound(String acc, char c, String tail, String replaceValue, Character... firstLetters) {
        if (Arrays.asList(firstLetters).contains(c)) {
            if (tail.charAt(0) == 'N' || tail.charAt(0) == 'M') {
                if (tail.length() > 1 && (tail.charAt(1) == 'N' || tail.charAt(1) == 'M' || tail.charAt(1) == 'H' || VOWELS.contains(tail.charAt(1)))) {
                    return operatePhonetic(acc + c, tail.charAt(0), substring(tail, 1, tail.length()));
                }
                return operatePhonetic(acc + replaceValue, charAt(tail, 1), substring(tail, 2, tail.length()));
            }
        }
        return null;
    }

    protected String substring(String tail, int startIndex, int endIndex) {
        if (tail == null) {
            return "";
        }
        if (tail.length() <= startIndex) {
            return "";
        } else {
            if (endIndex > tail.length() - 1) {
                return tail.substring(startIndex, tail.length());
            } else {
                return tail.substring(startIndex, endIndex);
            }
        }
    }

    protected Character charAt(String tail, int position) {
        if (tail == null || tail.isEmpty() || position < 0 || position >= tail.length()) {
            return null;
        }
        return tail.charAt(position);
    }

    @Override
    public Object encode(Object str) throws EncoderException {
        if (str instanceof String) {
            return encode((String) str);
        }
        throw new EncoderException("Object must be a String to be parsed");
    }

    static String clean(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }
        int len = str.length();
        char[] chars = new char[len];
        int count = 0;
        for (int i = 0; i < len; i++) {
            if (Character.isLetter(str.charAt(i))) {
                chars[count++] = str.charAt(i);
            }
        }
        if (count == len) {
            return str.toUpperCase(Locale.FRENCH);
        }
        return new String(chars, 0, count).toUpperCase(java.util.Locale.FRENCH);
    }
}

