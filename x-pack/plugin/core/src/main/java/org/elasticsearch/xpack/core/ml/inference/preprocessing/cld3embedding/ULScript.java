/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding;

public enum ULScript {
    ULScript_Common(0),//Zyyy
    ULScript_Latin(1),//Latn
    ULScript_Greek(2),//Grek
    ULScript_Cyrillic(3),//Cyrl
    ULScript_Armenian(4),//Armn
    ULScript_Hebrew(5),//Hebr
    ULScript_Arabic(6),//Arab
    ULScript_Syriac(7),//Syrc
    ULScript_Thaana(8),//Thaa
    ULScript_Devanagari(9),//Deva
    ULScript_Bengali(10),//Beng
    ULScript_Gurmukhi(11),//Guru
    ULScript_Gujarati(12),//Gujr
    ULScript_Oriya(13),//Orya
    ULScript_Tamil(14),//Taml
    ULScript_Telugu(15),//Telu
    ULScript_Kannada(16),//Knda
    ULScript_Malayalam(17),//Mlym
    ULScript_Sinhala(18),//Sinh
    ULScript_Thai(19),//Thai
    ULScript_Lao(20),//Laoo
    ULScript_Tibetan(21),//Tibt
    ULScript_Myanmar(22),//Mymr
    ULScript_Georgian(23),//Geor
    ULScript_Hani(24),//Hani
    ULScript_Ethiopic(25),//Ethi
    ULScript_Cherokee(26),//Cher
    ULScript_Canadian_Aboriginal(27),//Cans
    ULScript_Ogham(28),//Ogam
    ULScript_Runic(29),//Runr
    ULScript_Khmer(30),//Khmr
    ULScript_Mongolian(31),//Mong
    ULScript_32(32),//
    ULScript_33(33),//
    ULScript_Bopomofo(34),//Bopo
    ULScript_35(35),//
    ULScript_Yi(36),//Yiii
    ULScript_Old_Italic(37),//Ital
    ULScript_Gothic(38),//Goth
    ULScript_Deseret(39),//Dsrt
    ULScript_Inherited(40),//Zinh
    ULScript_Tagalog(41),//Tglg
    ULScript_Hanunoo(42),//Hano
    ULScript_Buhid(43),//Buhd
    ULScript_Tagbanwa(44),//Tagb
    ULScript_Limbu(45),//Limb
    ULScript_Tai_Le(46),//Tale
    ULScript_Linear_B(47),//Linb
    ULScript_Ugaritic(48),//Ugar
    ULScript_Shavian(49),//Shaw
    ULScript_Osmanya(50),//Osma
    ULScript_Cypriot(51),//Cprt
    ULScript_Braille(52),//Brai
    ULScript_Buginese(53),//Bugi
    ULScript_Coptic(54),//Copt
    ULScript_New_Tai_Lue(55),//Talu
    ULScript_Glagolitic(56),//Glag
    ULScript_Tifinagh(57),//Tfng
    ULScript_Syloti_Nagri(58),//Sylo
    ULScript_Old_Persian(59),//Xpeo
    ULScript_Kharoshthi(60),//Khar
    ULScript_Balinese(61),//Bali
    ULScript_Cuneiform(62),//Xsux
    ULScript_Phoenician(63),//Phnx
    ULScript_Phags_Pa(64),//Phag
    ULScript_Nko(65),//Nkoo
    ULScript_Sundanese(66),//Sund
    ULScript_Lepcha(67),//Lepc
    ULScript_Ol_Chiki(68),//Olck
    ULScript_Vai(69),//Vaii
    ULScript_Saurashtra(70),//Saur
    ULScript_Kayah_Li(71),//Kali
    ULScript_Rejang(72),//Rjng
    ULScript_Lycian(73),//Lyci
    ULScript_Carian(74),//Cari
    ULScript_Lydian(75),//Lydi
    ULScript_Cham(76),//Cham
    ULScript_Tai_Tham(77),//Lana
    ULScript_Tai_Viet(78),//Tavt
    ULScript_Avestan(79),//Avst
    ULScript_Egyptian_Hieroglyphs(80),//Egyp
    ULScript_Samaritan(81),//Samr
    ULScript_Lisu(82),//Lisu
    ULScript_Bamum(83),//Bamu
    ULScript_Javanese(84),//Java
    ULScript_Meetei_Mayek(85),//Mtei
    ULScript_Imperial_Aramaic(86),//Armi
    ULScript_Old_South_Arabian(87),//Sarb
    ULScript_Inscriptional_Parthian(88),//Prti
    ULScript_Inscriptional_Pahlavi(89),//Phli
    ULScript_Old_Turkic(90),//Orkh
    ULScript_Kaithi(91),//Kthi
    ULScript_Batak(92),//Batk
    ULScript_Brahmi(93),//Brah
    ULScript_Mandaic(94),//Mand
    ULScript_Chakma(95),//Cakm
    ULScript_Meroitic_Cursive(96),//Merc
    ULScript_Meroitic_Hieroglyphs(97),//Mero
    ULScript_Miao(98),//Plrd
    ULScript_Sharada(99),//Shrd
    ULScript_Sora_Sompeng(100),//Sora
    ULScript_Takri(101),//Takr
    NUM_ULSCRIPTS(102);

    private final int code;

    ULScript(int code) {
        this.code = code;
    }

    public static ULScript unicodeScriptToULScript(Character.UnicodeScript scriptId) {
        switch (scriptId) {
            case COMMON:
                return ULScript.ULScript_Common;
            case LATIN:
                return ULScript.ULScript_Latin;
            case GREEK:
                return ULScript.ULScript_Greek;
            case CYRILLIC:
                return ULScript.ULScript_Cyrillic;
            case ARMENIAN:
                return ULScript.ULScript_Armenian;
            case HEBREW:
                return ULScript.ULScript_Hebrew;
            case ARABIC:
                return ULScript.ULScript_Arabic;
            case SYRIAC:
                return ULScript.ULScript_Syriac;
            case THAANA:
                return ULScript.ULScript_Thaana;
            case DEVANAGARI:
                return ULScript.ULScript_Devanagari;
            case BENGALI:
                return ULScript.ULScript_Bengali;
            case GURMUKHI:
                return ULScript.ULScript_Gurmukhi;
            case GUJARATI:
                return ULScript.ULScript_Gujarati;
            case ORIYA:
                return ULScript.ULScript_Oriya;
            case TAMIL:
                return ULScript.ULScript_Tamil;
            case TELUGU:
                return ULScript.ULScript_Telugu;
            case KANNADA:
                return ULScript.ULScript_Kannada;
            case MALAYALAM:
                return ULScript.ULScript_Malayalam;
            case SINHALA:
                return ULScript.ULScript_Sinhala;
            case THAI:
                return ULScript.ULScript_Thai;
            case LAO:
                return ULScript.ULScript_Lao;
            case TIBETAN:
                return ULScript.ULScript_Tibetan;
            case MYANMAR:
                return ULScript.ULScript_Myanmar;
            case GEORGIAN:
                return ULScript.ULScript_Georgian;
            case HANGUL:
            case HAN:       // (based on testing cld3 vs java codepoints)
            case HIRAGANA:  // (based on testing cld3 va java codepoints)
            case KATAKANA:  // (based on testing cld3 va java codepoints)
                return ULScript.ULScript_Hani;
            case ETHIOPIC:
                return ULScript.ULScript_Ethiopic;
            case CHEROKEE:
                return ULScript.ULScript_Cherokee;
            case CANADIAN_ABORIGINAL:
                return ULScript.ULScript_Canadian_Aboriginal;
            case OGHAM:
                return ULScript.ULScript_Ogham;
            case RUNIC:
                return ULScript.ULScript_Runic;
            case KHMER:
                return ULScript.ULScript_Khmer;
            case MONGOLIAN:
                return ULScript.ULScript_Mongolian;
            case BOPOMOFO:
                return ULScript.ULScript_Bopomofo;
            case YI:
                return ULScript.ULScript_Yi;
            case OLD_ITALIC:
                return ULScript.ULScript_Old_Italic;
            case GOTHIC:
                return ULScript.ULScript_Gothic;
            case DESERET:
                return ULScript.ULScript_Deseret;
            case INHERITED:
                return ULScript.ULScript_Inherited;
            case TAGALOG:
                return ULScript.ULScript_Tagalog;
            case HANUNOO:
                return ULScript.ULScript_Hanunoo;
            case BUHID:
                return ULScript.ULScript_Buhid;
            case TAGBANWA:
                return ULScript.ULScript_Tagbanwa;
            case LIMBU:
                return ULScript.ULScript_Limbu;
            case TAI_LE:
                return ULScript.ULScript_Tai_Le;
            case LINEAR_B:
                return ULScript.ULScript_Linear_B;
            case UGARITIC:
                return ULScript.ULScript_Ugaritic;
            case SHAVIAN:
                return ULScript.ULScript_Shavian;
            case OSMANYA:
                return ULScript.ULScript_Osmanya;
            case CYPRIOT:
                return ULScript.ULScript_Cypriot;
            case BRAILLE:
                return ULScript.ULScript_Braille;
            case BUGINESE:
                return ULScript.ULScript_Buginese;
            case COPTIC:
                return ULScript.ULScript_Coptic;
            case NEW_TAI_LUE:
                return ULScript.ULScript_New_Tai_Lue;
            case GLAGOLITIC:
                return ULScript.ULScript_Glagolitic;
            case TIFINAGH:
                return ULScript.ULScript_Tifinagh;
            case SYLOTI_NAGRI:
                return ULScript.ULScript_Syloti_Nagri;
            case OLD_PERSIAN:
                return ULScript.ULScript_Old_Persian;
            case KHAROSHTHI:
                return ULScript.ULScript_Kharoshthi;
            case BALINESE:
                return ULScript.ULScript_Balinese;
            case CUNEIFORM:
                return ULScript.ULScript_Cuneiform;
            case PHOENICIAN:
                return ULScript.ULScript_Phoenician;
            case PHAGS_PA:
                return ULScript.ULScript_Phags_Pa;
            case NKO:
                return ULScript.ULScript_Nko;
            case SUNDANESE:
                return ULScript.ULScript_Sundanese;
            case LEPCHA:
                return ULScript.ULScript_Lepcha;
            case OL_CHIKI:
                return ULScript.ULScript_Ol_Chiki;
            case VAI:
                return ULScript.ULScript_Vai;
            case SAURASHTRA:
                return ULScript.ULScript_Saurashtra;
            case KAYAH_LI:
                return ULScript.ULScript_Kayah_Li;
            case REJANG:
                return ULScript.ULScript_Rejang;
            case LYCIAN:
                return ULScript.ULScript_Lycian;
            case CARIAN:
                return ULScript.ULScript_Carian;
            case LYDIAN:
                return ULScript.ULScript_Lydian;
            case CHAM:
                return ULScript.ULScript_Cham;
            case TAI_THAM:
                return ULScript.ULScript_Tai_Tham;
            case TAI_VIET:
                return ULScript.ULScript_Tai_Viet;
            case AVESTAN:
                return ULScript.ULScript_Avestan;
            case EGYPTIAN_HIEROGLYPHS:
                return ULScript.ULScript_Egyptian_Hieroglyphs;
            case SAMARITAN:
                return ULScript.ULScript_Samaritan;
            case LISU:
                return ULScript.ULScript_Lisu;
            case BAMUM:
                return ULScript.ULScript_Bamum;
            case JAVANESE:
                return ULScript.ULScript_Javanese;
            case MEETEI_MAYEK:
                return ULScript.ULScript_Meetei_Mayek;
            case IMPERIAL_ARAMAIC:
                return ULScript.ULScript_Imperial_Aramaic;
            case OLD_SOUTH_ARABIAN:
                return ULScript.ULScript_Old_South_Arabian;
            case INSCRIPTIONAL_PARTHIAN:
                return ULScript.ULScript_Inscriptional_Parthian;
            case INSCRIPTIONAL_PAHLAVI:
                return ULScript.ULScript_Inscriptional_Pahlavi;
            case OLD_TURKIC:
                return ULScript.ULScript_Old_Turkic;
            case KAITHI:
                return ULScript.ULScript_Kaithi;
            case BATAK:
                return ULScript.ULScript_Batak;
            case BRAHMI:
                return ULScript.ULScript_Brahmi;
            case MANDAIC:
                return ULScript.ULScript_Mandaic;
            case MEROITIC_CURSIVE:
                return ULScript.ULScript_Meroitic_Cursive;
            case MEROITIC_HIEROGLYPHS:
                return ULScript.ULScript_Meroitic_Hieroglyphs;
            case CHAKMA:
                return ULScript.ULScript_Chakma;
            case SHARADA:
                return ULScript.ULScript_Sharada;
            case SORA_SOMPENG:
                return ULScript.ULScript_Sora_Sompeng;
            case MIAO:
                return ULScript.ULScript_Miao;
            case TAKRI:
                return ULScript.ULScript_Takri;
            case UNKNOWN:
            default:
        }
        // Fall-through for unknown(s)
        return ULScript.ULScript_Common;
    }

    public int toInt() {
        return code;
    }
}
