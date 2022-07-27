/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

/**
 * These are the custom script codes that match up to the appropriate id row for the CLD3 weights and quantiles
 *
 * See https://github.com/google/cld3/blob/master/src/script_span/generated_ulscript.h
 */
public enum ScriptCode {
    Common(0),// Zyyy
    Latin(1),// Latn
    Greek(2),// Grek
    Cyrillic(3),// Cyrl
    Armenian(4),// Armn
    Hebrew(5),// Hebr
    Arabic(6),// Arab
    Syriac(7),// Syrc
    Thaana(8),// Thaa
    Devanagari(9),// Deva
    Bengali(10),// Beng
    Gurmukhi(11),// Guru
    Gujarati(12),// Gujr
    Oriya(13),// Orya
    Tamil(14),// Taml
    Telugu(15),// Telu
    Kannada(16),// Knda
    Malayalam(17),// Mlym
    Sinhala(18),// Sinh
    Thai(19),// Thai
    Lao(20),// Laoo
    Tibetan(21),// Tibt
    Myanmar(22),// Mymr
    Georgian(23),// Geor
    Hani(24),// Hani
    Ethiopic(25),// Ethi
    Cherokee(26),// Cher
    Canadian_Aboriginal(27),// Cans
    Ogham(28),// Ogam
    Runic(29),// Runr
    Khmer(30),// Khmr
    Mongolian(31),// Mong
    Undefined_32(32),//
    Undefined_33(33),//
    Bopomofo(34),// Bopo
    Undefined_35(35),//
    Yi(36),// Yiii
    Old_Italic(37),// Ital
    Gothic(38),// Goth
    Deseret(39),// Dsrt
    Inherited(40),// Zinh
    Tagalog(41),// Tglg
    Hanunoo(42),// Hano
    Buhid(43),// Buhd
    Tagbanwa(44),// Tagb
    Limbu(45),// Limb
    Tai_Le(46),// Tale
    Linear_B(47),// Linb
    Ugaritic(48),// Ugar
    Shavian(49),// Shaw
    Osmanya(50),// Osma
    Cypriot(51),// Cprt
    Braille(52),// Brai
    Buginese(53),// Bugi
    Coptic(54),// Copt
    New_Tai_Lue(55),// Talu
    Glagolitic(56),// Glag
    Tifinagh(57),// Tfng
    Syloti_Nagri(58),// Sylo
    Old_Persian(59),// Xpeo
    Kharoshthi(60),// Khar
    Balinese(61),// Bali
    Cuneiform(62),// Xsux
    Phoenician(63),// Phnx
    Phags_Pa(64),// Phag
    Nko(65),// Nkoo
    Sundanese(66),// Sund
    Lepcha(67),// Lepc
    Ol_Chiki(68),// Olck
    Vai(69),// Vaii
    Saurashtra(70),// Saur
    Kayah_Li(71),// Kali
    Rejang(72),// Rjng
    Lycian(73),// Lyci
    Carian(74),// Cari
    Lydian(75),// Lydi
    Cham(76),// Cham
    Tai_Tham(77),// Lana
    Tai_Viet(78),// Tavt
    Avestan(79),// Avst
    Egyptian_Hieroglyphs(80),// Egyp
    Samaritan(81),// Samr
    Lisu(82),// Lisu
    Bamum(83),// Bamu
    Javanese(84),// Java
    Meetei_Mayek(85),// Mtei
    Imperial_Aramaic(86),// Armi
    Old_South_Arabian(87),// Sarb
    Inscriptional_Parthian(88),// Prti
    Inscriptional_Pahlavi(89),// Phli
    Old_Turkic(90),// Orkh
    Kaithi(91),// Kthi
    Batak(92),// Batk
    Brahmi(93),// Brah
    Mandaic(94),// Mand
    Chakma(95),// Cakm
    Meroitic_Cursive(96),// Merc
    Meroitic_Hieroglyphs(97),// Mero
    Miao(98),// Plrd
    Sharada(99),// Shrd
    Sora_Sompeng(100),// Sora
    Takri(101),// Takr
    MAX_SCRIPT_CODE(102);

    private final int code;

    ScriptCode(int code) {
        this.code = code;
    }

    public static ScriptCode unicodeScriptToULScript(Character.UnicodeScript scriptId) {
        switch (scriptId) {
            case COMMON:
                return ScriptCode.Common;
            case LATIN:
                return ScriptCode.Latin;
            case GREEK:
                return ScriptCode.Greek;
            case CYRILLIC:
                return ScriptCode.Cyrillic;
            case ARMENIAN:
                return ScriptCode.Armenian;
            case HEBREW:
                return ScriptCode.Hebrew;
            case ARABIC:
                return ScriptCode.Arabic;
            case SYRIAC:
                return ScriptCode.Syriac;
            case THAANA:
                return ScriptCode.Thaana;
            case DEVANAGARI:
                return ScriptCode.Devanagari;
            case BENGALI:
                return ScriptCode.Bengali;
            case GURMUKHI:
                return ScriptCode.Gurmukhi;
            case GUJARATI:
                return ScriptCode.Gujarati;
            case ORIYA:
                return ScriptCode.Oriya;
            case TAMIL:
                return ScriptCode.Tamil;
            case TELUGU:
                return ScriptCode.Telugu;
            case KANNADA:
                return ScriptCode.Kannada;
            case MALAYALAM:
                return ScriptCode.Malayalam;
            case SINHALA:
                return ScriptCode.Sinhala;
            case THAI:
                return ScriptCode.Thai;
            case LAO:
                return ScriptCode.Lao;
            case TIBETAN:
                return ScriptCode.Tibetan;
            case MYANMAR:
                return ScriptCode.Myanmar;
            case GEORGIAN:
                return ScriptCode.Georgian;
            case HANGUL:
            case HAN:       // (based on testing cld3 vs java codepoints)
            case HIRAGANA:  // (based on testing cld3 va java codepoints)
            case KATAKANA:  // (based on testing cld3 va java codepoints)
                return ScriptCode.Hani;
            case ETHIOPIC:
                return ScriptCode.Ethiopic;
            case CHEROKEE:
                return ScriptCode.Cherokee;
            case CANADIAN_ABORIGINAL:
                return ScriptCode.Canadian_Aboriginal;
            case OGHAM:
                return ScriptCode.Ogham;
            case RUNIC:
                return ScriptCode.Runic;
            case KHMER:
                return ScriptCode.Khmer;
            case MONGOLIAN:
                return ScriptCode.Mongolian;
            case BOPOMOFO:
                return ScriptCode.Bopomofo;
            case YI:
                return ScriptCode.Yi;
            case OLD_ITALIC:
                return ScriptCode.Old_Italic;
            case GOTHIC:
                return ScriptCode.Gothic;
            case DESERET:
                return ScriptCode.Deseret;
            case INHERITED:
                return ScriptCode.Inherited;
            case TAGALOG:
                return ScriptCode.Tagalog;
            case HANUNOO:
                return ScriptCode.Hanunoo;
            case BUHID:
                return ScriptCode.Buhid;
            case TAGBANWA:
                return ScriptCode.Tagbanwa;
            case LIMBU:
                return ScriptCode.Limbu;
            case TAI_LE:
                return ScriptCode.Tai_Le;
            case LINEAR_B:
                return ScriptCode.Linear_B;
            case UGARITIC:
                return ScriptCode.Ugaritic;
            case SHAVIAN:
                return ScriptCode.Shavian;
            case OSMANYA:
                return ScriptCode.Osmanya;
            case CYPRIOT:
                return ScriptCode.Cypriot;
            case BRAILLE:
                return ScriptCode.Braille;
            case BUGINESE:
                return ScriptCode.Buginese;
            case COPTIC:
                return ScriptCode.Coptic;
            case NEW_TAI_LUE:
                return ScriptCode.New_Tai_Lue;
            case GLAGOLITIC:
                return ScriptCode.Glagolitic;
            case TIFINAGH:
                return ScriptCode.Tifinagh;
            case SYLOTI_NAGRI:
                return ScriptCode.Syloti_Nagri;
            case OLD_PERSIAN:
                return ScriptCode.Old_Persian;
            case KHAROSHTHI:
                return ScriptCode.Kharoshthi;
            case BALINESE:
                return ScriptCode.Balinese;
            case CUNEIFORM:
                return ScriptCode.Cuneiform;
            case PHOENICIAN:
                return ScriptCode.Phoenician;
            case PHAGS_PA:
                return ScriptCode.Phags_Pa;
            case NKO:
                return ScriptCode.Nko;
            case SUNDANESE:
                return ScriptCode.Sundanese;
            case LEPCHA:
                return ScriptCode.Lepcha;
            case OL_CHIKI:
                return ScriptCode.Ol_Chiki;
            case VAI:
                return ScriptCode.Vai;
            case SAURASHTRA:
                return ScriptCode.Saurashtra;
            case KAYAH_LI:
                return ScriptCode.Kayah_Li;
            case REJANG:
                return ScriptCode.Rejang;
            case LYCIAN:
                return ScriptCode.Lycian;
            case CARIAN:
                return ScriptCode.Carian;
            case LYDIAN:
                return ScriptCode.Lydian;
            case CHAM:
                return ScriptCode.Cham;
            case TAI_THAM:
                return ScriptCode.Tai_Tham;
            case TAI_VIET:
                return ScriptCode.Tai_Viet;
            case AVESTAN:
                return ScriptCode.Avestan;
            case EGYPTIAN_HIEROGLYPHS:
                return ScriptCode.Egyptian_Hieroglyphs;
            case SAMARITAN:
                return ScriptCode.Samaritan;
            case LISU:
                return ScriptCode.Lisu;
            case BAMUM:
                return ScriptCode.Bamum;
            case JAVANESE:
                return ScriptCode.Javanese;
            case MEETEI_MAYEK:
                return ScriptCode.Meetei_Mayek;
            case IMPERIAL_ARAMAIC:
                return ScriptCode.Imperial_Aramaic;
            case OLD_SOUTH_ARABIAN:
                return ScriptCode.Old_South_Arabian;
            case INSCRIPTIONAL_PARTHIAN:
                return ScriptCode.Inscriptional_Parthian;
            case INSCRIPTIONAL_PAHLAVI:
                return ScriptCode.Inscriptional_Pahlavi;
            case OLD_TURKIC:
                return ScriptCode.Old_Turkic;
            case KAITHI:
                return ScriptCode.Kaithi;
            case BATAK:
                return ScriptCode.Batak;
            case BRAHMI:
                return ScriptCode.Brahmi;
            case MANDAIC:
                return ScriptCode.Mandaic;
            case MEROITIC_CURSIVE:
                return ScriptCode.Meroitic_Cursive;
            case MEROITIC_HIEROGLYPHS:
                return ScriptCode.Meroitic_Hieroglyphs;
            case CHAKMA:
                return ScriptCode.Chakma;
            case SHARADA:
                return ScriptCode.Sharada;
            case SORA_SOMPENG:
                return ScriptCode.Sora_Sompeng;
            case MIAO:
                return ScriptCode.Miao;
            case TAKRI:
                return ScriptCode.Takri;
            case UNKNOWN:
            default:
        }
        // Fall-through for unknown(s)
        return ScriptCode.Common;
    }

    public int toInt() {
        return code;
    }
}
