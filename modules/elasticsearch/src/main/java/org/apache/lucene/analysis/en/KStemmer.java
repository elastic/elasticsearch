/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
This file was partially derived from the
original CIIR University of Massachusetts Amherst version of KStemmer.java (license for
the original shown below)
 */

/*
 Copyright Â© 2003,
 Center for Intelligent Information Retrieval,
 University of Massachusetts, Amherst.
 All rights reserved.

 Redistribution and use in source and binary forms, with or without modification,
 are permitted provided that the following conditions are met:

 1. Redistributions of source code must retain the above copyright notice, this
 list of conditions and the following disclaimer.

 2. Redistributions in binary form must reproduce the above copyright notice,
 this list of conditions and the following disclaimer in the documentation
 and/or other materials provided with the distribution.

 3. The names "Center for Intelligent Information Retrieval" and
 "University of Massachusetts" must not be used to endorse or promote products
 derived from this software without prior written permission. To obtain
 permission, contact info@ciir.cs.umass.edu.

 THIS SOFTWARE IS PROVIDED BY UNIVERSITY OF MASSACHUSETTS AND OTHER CONTRIBUTORS
 "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 SUCH DAMAGE.
 */
package org.apache.lucene.analysis.en;

import org.apache.lucene.analysis.CharArrayMap;
import org.apache.lucene.analysis.util.OpenStringBuilder;
/**
 * <p>Title: Kstemmer</p>
 * <p>Description: This is a java version of Bob Krovetz' kstem stemmer</p>
 * <p>Copyright: Copyright 2008, Luicid Imagination, Inc. </p>
 * <p>Copyright: Copyright 2003, CIIR University of Massachusetts Amherst (http://ciir.cs.umass.edu) </p>
 */
import org.apache.lucene.util.Version;

/**
 * This class implements the Kstem algorithm
 */
// LUCENE MONITOR: Remove as of Lucene 3.3
public class KStemmer {
  static private final int MaxWordLen = 50;
  
  static private final String[] exceptionWords = {"aide", "bathe", "caste",
      "cute", "dame", "dime", "doge", "done", "dune", "envelope", "gage",
      "grille", "grippe", "lobe", "mane", "mare", "nape", "node", "pane",
      "pate", "plane", "pope", "programme", "quite", "ripe", "rote", "rune",
      "sage", "severe", "shoppe", "sine", "slime", "snipe", "steppe", "suite",
      "swinge", "tare", "tine", "tope", "tripe", "twine"};
  
  static private final String[][] directConflations = { {"aging", "age"},
      {"going", "go"}, {"goes", "go"}, {"lying", "lie"}, {"using", "use"},
      {"owing", "owe"}, {"suing", "sue"}, {"dying", "die"}, {"tying", "tie"},
      {"vying", "vie"}, {"aged", "age"}, {"used", "use"}, {"vied", "vie"},
      {"cued", "cue"}, {"died", "die"}, {"eyed", "eye"}, {"hued", "hue"},
      {"iced", "ice"}, {"lied", "lie"}, {"owed", "owe"}, {"sued", "sue"},
      {"toed", "toe"}, {"tied", "tie"}, {"does", "do"}, {"doing", "do"},
      {"aeronautical", "aeronautics"}, {"mathematical", "mathematics"},
      {"political", "politics"}, {"metaphysical", "metaphysics"},
      {"cylindrical", "cylinder"}, {"nazism", "nazi"},
      {"ambiguity", "ambiguous"}, {"barbarity", "barbarous"},
      {"credulity", "credulous"}, {"generosity", "generous"},
      {"spontaneity", "spontaneous"}, {"unanimity", "unanimous"},
      {"voracity", "voracious"}, {"fled", "flee"}, {"miscarriage", "miscarry"}};
  
  static private final String[][] countryNationality = {
      {"afghan", "afghanistan"}, {"african", "africa"},
      {"albanian", "albania"}, {"algerian", "algeria"},
      {"american", "america"}, {"andorran", "andorra"}, {"angolan", "angola"},
      {"arabian", "arabia"}, {"argentine", "argentina"},
      {"armenian", "armenia"}, {"asian", "asia"}, {"australian", "australia"},
      {"austrian", "austria"}, {"azerbaijani", "azerbaijan"},
      {"azeri", "azerbaijan"}, {"bangladeshi", "bangladesh"},
      {"belgian", "belgium"}, {"bermudan", "bermuda"}, {"bolivian", "bolivia"},
      {"bosnian", "bosnia"}, {"botswanan", "botswana"},
      {"brazilian", "brazil"}, {"british", "britain"},
      {"bulgarian", "bulgaria"}, {"burmese", "burma"},
      {"californian", "california"}, {"cambodian", "cambodia"},
      {"canadian", "canada"}, {"chadian", "chad"}, {"chilean", "chile"},
      {"chinese", "china"}, {"colombian", "colombia"}, {"croat", "croatia"},
      {"croatian", "croatia"}, {"cuban", "cuba"}, {"cypriot", "cyprus"},
      {"czechoslovakian", "czechoslovakia"}, {"danish", "denmark"},
      {"egyptian", "egypt"}, {"equadorian", "equador"},
      {"eritrean", "eritrea"}, {"estonian", "estonia"},
      {"ethiopian", "ethiopia"}, {"european", "europe"}, {"fijian", "fiji"},
      {"filipino", "philippines"}, {"finnish", "finland"},
      {"french", "france"}, {"gambian", "gambia"}, {"georgian", "georgia"},
      {"german", "germany"}, {"ghanian", "ghana"}, {"greek", "greece"},
      {"grenadan", "grenada"}, {"guamian", "guam"},
      {"guatemalan", "guatemala"}, {"guinean", "guinea"},
      {"guyanan", "guyana"}, {"haitian", "haiti"}, {"hawaiian", "hawaii"},
      {"holland", "dutch"}, {"honduran", "honduras"}, {"hungarian", "hungary"},
      {"icelandic", "iceland"}, {"indonesian", "indonesia"},
      {"iranian", "iran"}, {"iraqi", "iraq"}, {"iraqui", "iraq"},
      {"irish", "ireland"}, {"israeli", "israel"},
      {"italian", "italy"},
      {"jamaican", "jamaica"},
      {"japanese", "japan"},
      {"jordanian", "jordan"},
      {"kampuchean", "cambodia"},
      {"kenyan", "kenya"},
      {"korean", "korea"},
      {"kuwaiti", "kuwait"},
      {"lankan", "lanka"},
      {"laotian", "laos"},
      {"latvian", "latvia"},
      {"lebanese", "lebanon"},
      {"liberian", "liberia"},
      {"libyan", "libya"},
      {"lithuanian", "lithuania"},
      {"macedonian", "macedonia"},
      {"madagascan", "madagascar"},
      {"malaysian", "malaysia"},
      {"maltese", "malta"},
      {"mauritanian", "mauritania"},
      {"mexican", "mexico"},
      {"micronesian", "micronesia"},
      {"moldovan", "moldova"},
      {"monacan", "monaco"},
      {"mongolian", "mongolia"},
      {"montenegran", "montenegro"},
      {"moroccan", "morocco"},
      {"myanmar", "burma"},
      {"namibian", "namibia"},
      {"nepalese", "nepal"},
      // {"netherlands", "dutch"},
      {"nicaraguan", "nicaragua"}, {"nigerian", "nigeria"},
      {"norwegian", "norway"}, {"omani", "oman"}, {"pakistani", "pakistan"},
      {"panamanian", "panama"}, {"papuan", "papua"},
      {"paraguayan", "paraguay"}, {"peruvian", "peru"},
      {"portuguese", "portugal"}, {"romanian", "romania"},
      {"rumania", "romania"}, {"rumanian", "romania"}, {"russian", "russia"},
      {"rwandan", "rwanda"}, {"samoan", "samoa"}, {"scottish", "scotland"},
      {"serb", "serbia"}, {"serbian", "serbia"}, {"siam", "thailand"},
      {"siamese", "thailand"}, {"slovakia", "slovak"}, {"slovakian", "slovak"},
      {"slovenian", "slovenia"}, {"somali", "somalia"},
      {"somalian", "somalia"}, {"spanish", "spain"}, {"swedish", "sweden"},
      {"swiss", "switzerland"}, {"syrian", "syria"}, {"taiwanese", "taiwan"},
      {"tanzanian", "tanzania"}, {"texan", "texas"}, {"thai", "thailand"},
      {"tunisian", "tunisia"}, {"turkish", "turkey"}, {"ugandan", "uganda"},
      {"ukrainian", "ukraine"}, {"uruguayan", "uruguay"},
      {"uzbek", "uzbekistan"}, {"venezuelan", "venezuela"},
      {"vietnamese", "viet"}, {"virginian", "virginia"}, {"yemeni", "yemen"},
      {"yugoslav", "yugoslavia"}, {"yugoslavian", "yugoslavia"},
      {"zambian", "zambia"}, {"zealander", "zealand"},
      {"zimbabwean", "zimbabwe"}};
  
  static private final String[] supplementDict = {"aids", "applicator",
      "capacitor", "digitize", "electromagnet", "ellipsoid", "exosphere",
      "extensible", "ferromagnet", "graphics", "hydromagnet", "polygraph",
      "toroid", "superconduct", "backscatter", "connectionism"};
  
  static private final String[] properNouns = {"abrams", "achilles",
      "acropolis", "adams", "agnes", "aires", "alexander", "alexis", "alfred",
      "algiers", "alps", "amadeus", "ames", "amos", "andes", "angeles",
      "annapolis", "antilles", "aquarius", "archimedes", "arkansas", "asher",
      "ashly", "athens", "atkins", "atlantis", "avis", "bahamas", "bangor",
      "barbados", "barger", "bering", "brahms", "brandeis", "brussels",
      "bruxelles", "cairns", "camoros", "camus", "carlos", "celts", "chalker",
      "charles", "cheops", "ching", "christmas", "cocos", "collins",
      "columbus", "confucius", "conners", "connolly", "copernicus", "cramer",
      "cyclops", "cygnus", "cyprus", "dallas", "damascus", "daniels", "davies",
      "davis", "decker", "denning", "dennis", "descartes", "dickens", "doris",
      "douglas", "downs", "dreyfus", "dukakis", "dulles", "dumfries",
      "ecclesiastes", "edwards", "emily", "erasmus", "euphrates", "evans",
      "everglades", "fairbanks", "federales", "fisher", "fitzsimmons",
      "fleming", "forbes", "fowler", "france", "francis", "goering",
      "goodling", "goths", "grenadines", "guiness", "hades", "harding",
      "harris", "hastings", "hawkes", "hawking", "hayes", "heights",
      "hercules", "himalayas", "hippocrates", "hobbs", "holmes", "honduras",
      "hopkins", "hughes", "humphreys", "illinois", "indianapolis",
      "inverness", "iris", "iroquois", "irving", "isaacs", "italy", "james",
      "jarvis", "jeffreys", "jesus", "jones", "josephus", "judas", "julius",
      "kansas", "keynes", "kipling", "kiwanis", "lansing", "laos", "leeds",
      "levis", "leviticus", "lewis", "louis", "maccabees", "madras",
      "maimonides", "maldive", "massachusetts", "matthews", "mauritius",
      "memphis", "mercedes", "midas", "mingus", "minneapolis", "mohammed",
      "moines", "morris", "moses", "myers", "myknos", "nablus", "nanjing",
      "nantes", "naples", "neal", "netherlands", "nevis", "nostradamus",
      "oedipus", "olympus", "orleans", "orly", "papas", "paris", "parker",
      "pauling", "peking", "pershing", "peter", "peters", "philippines",
      "phineas", "pisces", "pryor", "pythagoras", "queens", "rabelais",
      "ramses", "reynolds", "rhesus", "rhodes", "richards", "robins",
      "rodgers", "rogers", "rubens", "sagittarius", "seychelles", "socrates",
      "texas", "thames", "thomas", "tiberias", "tunis", "venus", "vilnius",
      "wales", "warner", "wilkins", "williams", "wyoming", "xmas", "yonkers",
      "zeus", "frances", "aarhus", "adonis", "andrews", "angus", "antares",
      "aquinas", "arcturus", "ares", "artemis", "augustus", "ayers",
      "barnabas", "barnes", "becker", "bejing", "biggs", "billings", "boeing",
      "boris", "borroughs", "briggs", "buenos", "calais", "caracas", "cassius",
      "cerberus", "ceres", "cervantes", "chantilly", "chartres", "chester",
      "connally", "conner", "coors", "cummings", "curtis", "daedalus",
      "dionysus", "dobbs", "dolores", "edmonds"};
  
  static class DictEntry {
    boolean exception;
    String root;
    
    DictEntry(String root, boolean isException) {
      this.root = root;
      this.exception = isException;
    }
  }
  
  private static final CharArrayMap<DictEntry> dict_ht = initializeDictHash();
  
  /***
   * caching off private int maxCacheSize; private CharArrayMap<String> cache =
   * null; private static final String SAME = "SAME"; // use if stemmed form is
   * the same
   ***/
  
  private final OpenStringBuilder word = new OpenStringBuilder();
  private int j; /* index of final letter in stem (within word) */
  private int k; /*
                  * INDEX of final letter in word. You must add 1 to k to get
                  * the current length of word. When you want the length of
                  * word, use the method wordLength, which returns (k+1).
                  */
  
  /***
   * private void initializeStemHash() { if (maxCacheSize > 0) cache = new
   * CharArrayMap<String>(maxCacheSize,false); }
   ***/
  
  private char finalChar() {
    return word.charAt(k);
  }
  
  private char penultChar() {
    return word.charAt(k - 1);
  }
  
  private boolean isVowel(int index) {
    return !isCons(index);
  }
  
  private boolean isCons(int index) {
    char ch;
    
    ch = word.charAt(index);
    
    if ((ch == 'a') || (ch == 'e') || (ch == 'i') || (ch == 'o') || (ch == 'u')) return false;
    if ((ch != 'y') || (index == 0)) return true;
    else return (!isCons(index - 1));
  }
  
  private static CharArrayMap<DictEntry> initializeDictHash() {
    DictEntry defaultEntry;
    DictEntry entry;

    CharArrayMap<DictEntry> d = new CharArrayMap<DictEntry>(
        Version.LUCENE_31, 1000, false);
    
    d = new CharArrayMap<DictEntry>(Version.LUCENE_31, 1000, false);
    for (int i = 0; i < exceptionWords.length; i++) {
      if (!d.containsKey(exceptionWords[i])) {
        entry = new DictEntry(exceptionWords[i], true);
        d.put(exceptionWords[i], entry);
      } else {
        System.out.println("Warning: Entry [" + exceptionWords[i]
            + "] already in dictionary 1");
      }
    }
    
    for (int i = 0; i < directConflations.length; i++) {
      if (!d.containsKey(directConflations[i][0])) {
        entry = new DictEntry(directConflations[i][1], false);
        d.put(directConflations[i][0], entry);
      } else {
        System.out.println("Warning: Entry [" + directConflations[i][0]
            + "] already in dictionary 2");
      }
    }
    
    for (int i = 0; i < countryNationality.length; i++) {
      if (!d.containsKey(countryNationality[i][0])) {
        entry = new DictEntry(countryNationality[i][1], false);
        d.put(countryNationality[i][0], entry);
      } else {
        System.out.println("Warning: Entry [" + countryNationality[i][0]
            + "] already in dictionary 3");
      }
    }
    
    defaultEntry = new DictEntry(null, false);
    
    String[] array;
    array = KStemData1.data;
    
    for (int i = 0; i < array.length; i++) {
      if (!d.containsKey(array[i])) {
        d.put(array[i], defaultEntry);
      } else {
        System.out.println("Warning: Entry [" + array[i]
            + "] already in dictionary 4");
      }
    }
    
    array = KStemData2.data;
    for (int i = 0; i < array.length; i++) {
      if (!d.containsKey(array[i])) {
        d.put(array[i], defaultEntry);
      } else {
        System.out.println("Warning: Entry [" + array[i]
            + "] already in dictionary 4");
      }
    }
    
    array = KStemData3.data;
    for (int i = 0; i < array.length; i++) {
      if (!d.containsKey(array[i])) {
        d.put(array[i], defaultEntry);
      } else {
        System.out.println("Warning: Entry [" + array[i]
            + "] already in dictionary 4");
      }
    }
    
    array = KStemData4.data;
    for (int i = 0; i < array.length; i++) {
      if (!d.containsKey(array[i])) {
        d.put(array[i], defaultEntry);
      } else {
        System.out.println("Warning: Entry [" + array[i]
            + "] already in dictionary 4");
      }
    }
    
    array = KStemData5.data;
    for (int i = 0; i < array.length; i++) {
      if (!d.containsKey(array[i])) {
        d.put(array[i], defaultEntry);
      } else {
        System.out.println("Warning: Entry [" + array[i]
            + "] already in dictionary 4");
      }
    }
    
    array = KStemData6.data;
    for (int i = 0; i < array.length; i++) {
      if (!d.containsKey(array[i])) {
        d.put(array[i], defaultEntry);
      } else {
        System.out.println("Warning: Entry [" + array[i]
            + "] already in dictionary 4");
      }
    }
    
    array = KStemData7.data;
    for (int i = 0; i < array.length; i++) {
      if (!d.containsKey(array[i])) {
        d.put(array[i], defaultEntry);
      } else {
        System.out.println("Warning: Entry [" + array[i]
            + "] already in dictionary 4");
      }
    }
    
    for (int i = 0; i < KStemData8.data.length; i++) {
      if (!d.containsKey(KStemData8.data[i])) {
        d.put(KStemData8.data[i], defaultEntry);
      } else {
        System.out.println("Warning: Entry [" + KStemData8.data[i]
            + "] already in dictionary 4");
      }
    }
    
    for (int i = 0; i < supplementDict.length; i++) {
      if (!d.containsKey(supplementDict[i])) {
        d.put(supplementDict[i], defaultEntry);
      } else {
        System.out.println("Warning: Entry [" + supplementDict[i]
            + "] already in dictionary 5");
      }
    }
    
    for (int i = 0; i < properNouns.length; i++) {
      if (!d.containsKey(properNouns[i])) {
        d.put(properNouns[i], defaultEntry);
      } else {
        System.out.println("Warning: Entry [" + properNouns[i]
            + "] already in dictionary 6");
      }
    }
    
    return d;
  }
  
  private boolean isAlpha(char ch) {
    return ch >= 'a' && ch <= 'z'; // terms must be lowercased already
  }
  
  /* length of stem within word */
  private int stemLength() {
    return j + 1;
  };
  
  private boolean endsIn(char[] s) {
    if (s.length > k) return false;
    
    int r = word.length() - s.length; /* length of word before this suffix */
    j = k;
    for (int r1 = r, i = 0; i < s.length; i++, r1++) {
      if (s[i] != word.charAt(r1)) return false;
    }
    j = r - 1; /* index of the character BEFORE the posfix */
    return true;
  }
  
  private boolean endsIn(char a, char b) {
    if (2 > k) return false;
    // check left to right since the endings have often already matched
    if (word.charAt(k - 1) == a && word.charAt(k) == b) {
      j = k - 2;
      return true;
    }
    return false;
  }
  
  private boolean endsIn(char a, char b, char c) {
    if (3 > k) return false;
    if (word.charAt(k - 2) == a && word.charAt(k - 1) == b
        && word.charAt(k) == c) {
      j = k - 3;
      return true;
    }
    return false;
  }
  
  private boolean endsIn(char a, char b, char c, char d) {
    if (4 > k) return false;
    if (word.charAt(k - 3) == a && word.charAt(k - 2) == b
        && word.charAt(k - 1) == c && word.charAt(k) == d) {
      j = k - 4;
      return true;
    }
    return false;
  }
  
  private DictEntry wordInDict() {
    /***
     * if (matchedEntry != null) { if (dict_ht.get(word.getArray(), 0,
     * word.size()) != matchedEntry) {
     * System.out.println("Uh oh... cached entry doesn't match"); } return
     * matchedEntry; }
     ***/
    if (matchedEntry != null) return matchedEntry;
    DictEntry e = dict_ht.get(word.getArray(), 0, word.length());
    if (e != null && !e.exception) {
      matchedEntry = e; // only cache if it's not an exception.
    }
    // lookups.add(word.toString());
    return e;
  }
  
  /* Convert plurals to singular form, and '-ies' to 'y' */
  private void plural() {
    if (word.charAt(k) == 's') {
      if (endsIn('i', 'e', 's')) {
        word.setLength(j + 3);
        k--;
        if (lookup()) /* ensure calories -> calorie */
        return;
        k++;
        word.unsafeWrite('s');
        setSuffix("y");
        lookup();
      } else if (endsIn('e', 's')) {
        /* try just removing the "s" */
        word.setLength(j + 2);
        k--;
        
        /*
         * note: don't check for exceptions here. So, `aides' -> `aide', but
         * `aided' -> `aid'. The exception for double s is used to prevent
         * crosses -> crosse. This is actually correct if crosses is a plural
         * noun (a type of racket used in lacrosse), but the verb is much more
         * common
         */

        /****
         * YCS: this was the one place where lookup was not followed by return.
         * So restructure it. if ((j>0)&&(lookup(word.toString())) &&
         * !((word.charAt(j) == 's') && (word.charAt(j-1) == 's'))) return;
         *****/
        boolean tryE = j > 0
            && !((word.charAt(j) == 's') && (word.charAt(j - 1) == 's'));
        if (tryE && lookup()) return;
        
        /* try removing the "es" */

        word.setLength(j + 1);
        k--;
        if (lookup()) return;
        
        /* the default is to retain the "e" */
        word.unsafeWrite('e');
        k++;
        
        if (!tryE) lookup(); // if we didn't try the "e" ending before
        return;
      } else {
        if (word.length() > 3 && penultChar() != 's' && !endsIn('o', 'u', 's')) {
          /* unless the word ends in "ous" or a double "s", remove the final "s" */

          word.setLength(k);
          k--;
          lookup();
        }
      }
    }
  }
  
  private void setSuffix(String s) {
    setSuff(s, s.length());
  }
  
  /* replace old suffix with s */
  private void setSuff(String s, int len) {
    word.setLength(j + 1);
    for (int l = 0; l < len; l++) {
      word.unsafeWrite(s.charAt(l));
    }
    k = j + len;
  }
  
  /* Returns true if the word is found in the dictionary */
  // almost all uses of lookup() return immediately and are
  // followed by another lookup in the dict. Store the match
  // to avoid this double lookup.
  DictEntry matchedEntry = null;
  
  private boolean lookup() {
    /******
     * debugging code String thisLookup = word.toString(); boolean added =
     * lookups.add(thisLookup); if (!added) {
     * System.out.println("######extra lookup:" + thisLookup); // occaasional
     * extra lookups aren't necessarily errors... could happen by diff
     * manipulations // throw new RuntimeException("######extra lookup:" +
     * thisLookup); } else { // System.out.println("new lookup:" + thisLookup);
     * }
     ******/
    
    matchedEntry = dict_ht.get(word.getArray(), 0, word.size());
    return matchedEntry != null;
  }
  
  // Set<String> lookups = new HashSet<String>();
  
  /* convert past tense (-ed) to present, and `-ied' to `y' */
  private void pastTense() {
    /*
     * Handle words less than 5 letters with a direct mapping This prevents
     * (fled -> fl).
     */
    if (word.length() <= 4) return;
    
    if (endsIn('i', 'e', 'd')) {
      word.setLength(j + 3);
      k--;
      if (lookup()) /* we almost always want to convert -ied to -y, but */
      return; /* this isn't true for short words (died->die) */
      k++; /* I don't know any long words that this applies to, */
      word.unsafeWrite('d'); /* but just in case... */
      setSuffix("y");
      lookup();
      return;
    }
    
    /* the vowelInStem() is necessary so we don't stem acronyms */
    if (endsIn('e', 'd') && vowelInStem()) {
      /* see if the root ends in `e' */
      word.setLength(j + 2);
      k = j + 1;
      
      DictEntry entry = wordInDict();
      if (entry != null) if (!entry.exception) /*
                                                * if it's in the dictionary and
                                                * not an exception
                                                */
      return;
      
      /* try removing the "ed" */
      word.setLength(j + 1);
      k = j;
      if (lookup()) return;
      
      /*
       * try removing a doubled consonant. if the root isn't found in the
       * dictionary, the default is to leave it doubled. This will correctly
       * capture `backfilled' -> `backfill' instead of `backfill' ->
       * `backfille', and seems correct most of the time
       */

      if (doubleC(k)) {
        word.setLength(k);
        k--;
        if (lookup()) return;
        word.unsafeWrite(word.charAt(k));
        k++;
        lookup();
        return;
      }
      
      /* if we have a `un-' prefix, then leave the word alone */
      /* (this will sometimes screw up with `under-', but we */
      /* will take care of that later) */

      if ((word.charAt(0) == 'u') && (word.charAt(1) == 'n')) {
        word.unsafeWrite('e');
        word.unsafeWrite('d');
        k = k + 2;
        // nolookup()
        return;
      }
      
      /*
       * it wasn't found by just removing the `d' or the `ed', so prefer to end
       * with an `e' (e.g., `microcoded' -> `microcode').
       */

      word.setLength(j + 1);
      word.unsafeWrite('e');
      k = j + 1;
      // nolookup() - we already tried the "e" ending
      return;
    }
  }
  
  /* return TRUE if word ends with a double consonant */
  private boolean doubleC(int i) {
    if (i < 1) return false;
    
    if (word.charAt(i) != word.charAt(i - 1)) return false;
    return (isCons(i));
  }
  
  private boolean vowelInStem() {
    for (int i = 0; i < stemLength(); i++) {
      if (isVowel(i)) return true;
    }
    return false;
  }
  
  /* handle `-ing' endings */
  private void aspect() {
    /*
     * handle short words (aging -> age) via a direct mapping. This prevents
     * (thing -> the) in the version of this routine that ignores inflectional
     * variants that are mentioned in the dictionary (when the root is also
     * present)
     */

    if (word.length() <= 5) return;
    
    /* the vowelinstem() is necessary so we don't stem acronyms */
    if (endsIn('i', 'n', 'g') && vowelInStem()) {
      
      /* try adding an `e' to the stem and check against the dictionary */
      word.setCharAt(j + 1, 'e');
      word.setLength(j + 2);
      k = j + 1;
      
      DictEntry entry = wordInDict();
      if (entry != null) {
        if (!entry.exception) /* if it's in the dictionary and not an exception */
        return;
      }
      
      /* adding on the `e' didn't work, so remove it */
      word.setLength(k);
      k--; /* note that `ing' has also been removed */
      
      if (lookup()) return;
      
      /* if I can remove a doubled consonant and get a word, then do so */
      if (doubleC(k)) {
        k--;
        word.setLength(k + 1);
        if (lookup()) return;
        word.unsafeWrite(word.charAt(k)); /* restore the doubled consonant */
        
        /* the default is to leave the consonant doubled */
        /* (e.g.,`fingerspelling' -> `fingerspell'). Unfortunately */
        /* `bookselling' -> `booksell' and `mislabelling' -> `mislabell'). */
        /* Without making the algorithm significantly more complicated, this */
        /* is the best I can do */
        k++;
        lookup();
        return;
      }
      
      /*
       * the word wasn't in the dictionary after removing the stem, and then
       * checking with and without a final `e'. The default is to add an `e'
       * unless the word ends in two consonants, so `microcoding' ->
       * `microcode'. The two consonants restriction wouldn't normally be
       * necessary, but is needed because we don't try to deal with prefixes and
       * compounds, and most of the time it is correct (e.g., footstamping ->
       * footstamp, not footstampe; however, decoupled -> decoupl). We can
       * prevent almost all of the incorrect stems if we try to do some prefix
       * analysis first
       */

      if ((j > 0) && isCons(j) && isCons(j - 1)) {
        k = j;
        word.setLength(k + 1);
        // nolookup() because we already did according to the comment
        return;
      }
      
      word.setLength(j + 1);
      word.unsafeWrite('e');
      k = j + 1;
      // nolookup(); we already tried an 'e' ending
      return;
    }
  }
  
  /*
   * this routine deals with -ity endings. It accepts -ability, -ibility, and
   * -ality, even without checking the dictionary because they are so
   * productive. The first two are mapped to -ble, and the -ity is remove for
   * the latter
   */
  private void ityEndings() {
    int old_k = k;
    
    if (endsIn('i', 't', 'y')) {
      word.setLength(j + 1); /* try just removing -ity */
      k = j;
      if (lookup()) return;
      word.unsafeWrite('e'); /* try removing -ity and adding -e */
      k = j + 1;
      if (lookup()) return;
      word.setCharAt(j + 1, 'i');
      word.append("ty");
      k = old_k;
      /*
       * the -ability and -ibility endings are highly productive, so just accept
       * them
       */
      if ((j > 0) && (word.charAt(j - 1) == 'i') && (word.charAt(j) == 'l')) {
        word.setLength(j - 1);
        word.append("le"); /* convert to -ble */
        k = j;
        lookup();
        return;
      }
      
      /* ditto for -ivity */
      if ((j > 0) && (word.charAt(j - 1) == 'i') && (word.charAt(j) == 'v')) {
        word.setLength(j + 1);
        word.unsafeWrite('e'); /* convert to -ive */
        k = j + 1;
        lookup();
        return;
      }
      /* ditto for -ality */
      if ((j > 0) && (word.charAt(j - 1) == 'a') && (word.charAt(j) == 'l')) {
        word.setLength(j + 1);
        k = j;
        lookup();
        return;
      }
      
      /*
       * if the root isn't in the dictionary, and the variant *is* there, then
       * use the variant. This allows `immunity'->`immune', but prevents
       * `capacity'->`capac'. If neither the variant nor the root form are in
       * the dictionary, then remove the ending as a default
       */

      if (lookup()) return;
      
      /* the default is to remove -ity altogether */
      word.setLength(j + 1);
      k = j;
      // nolookup(), we already did it.
      return;
    }
  }
  
  /* handle -ence and -ance */
  private void nceEndings() {
    int old_k = k;
    char word_char;
    
    if (endsIn('n', 'c', 'e')) {
      word_char = word.charAt(j);
      if (!((word_char == 'e') || (word_char == 'a'))) return;
      word.setLength(j);
      word.unsafeWrite('e'); /* try converting -e/ance to -e (adherance/adhere) */
      k = j;
      if (lookup()) return;
      word.setLength(j); /*
                          * try removing -e/ance altogether
                          * (disappearance/disappear)
                          */
      k = j - 1;
      if (lookup()) return;
      word.unsafeWrite(word_char); /* restore the original ending */
      word.append("nce");
      k = old_k;
      // nolookup() because we restored the original ending
    }
    return;
  }
  
  /* handle -ness */
  private void nessEndings() {
    if (endsIn('n', 'e', 's', 's')) { /*
                                       * this is a very productive endings, so
                                       * just accept it
                                       */
      word.setLength(j + 1);
      k = j;
      if (word.charAt(j) == 'i') word.setCharAt(j, 'y');
      lookup();
    }
    return;
  }
  
  /* handle -ism */
  private void ismEndings() {
    if (endsIn('i', 's', 'm')) { /*
                                  * this is a very productive ending, so just
                                  * accept it
                                  */
      word.setLength(j + 1);
      k = j;
      lookup();
    }
    return;
  }
  
  /* this routine deals with -ment endings. */
  private void mentEndings() {
    int old_k = k;
    
    if (endsIn('m', 'e', 'n', 't')) {
      word.setLength(j + 1);
      k = j;
      if (lookup()) return;
      word.append("ment");
      k = old_k;
      // nolookup
    }
    return;
  }
  
  /* this routine deals with -ize endings. */
  private void izeEndings() {
    int old_k = k;
    
    if (endsIn('i', 'z', 'e')) {
      word.setLength(j + 1); /* try removing -ize entirely */
      k = j;
      if (lookup()) return;
      word.unsafeWrite('i');
      
      if (doubleC(j)) { /* allow for a doubled consonant */
        word.setLength(j);
        k = j - 1;
        if (lookup()) return;
        word.unsafeWrite(word.charAt(j - 1));
      }
      
      word.setLength(j + 1);
      word.unsafeWrite('e'); /* try removing -ize and adding -e */
      k = j + 1;
      if (lookup()) return;
      word.setLength(j + 1);
      word.append("ize");
      k = old_k;
      // nolookup()
    }
    return;
  }
  
  /* handle -ency and -ancy */
  private void ncyEndings() {
    if (endsIn('n', 'c', 'y')) {
      if (!((word.charAt(j) == 'e') || (word.charAt(j) == 'a'))) return;
      word.setCharAt(j + 2, 't'); /* try converting -ncy to -nt */
      word.setLength(j + 3);
      k = j + 2;
      
      if (lookup()) return;
      
      word.setCharAt(j + 2, 'c'); /* the default is to convert it to -nce */
      word.unsafeWrite('e');
      k = j + 3;
      lookup();
    }
    return;
  }
  
  /* handle -able and -ible */
  private void bleEndings() {
    int old_k = k;
    char word_char;
    
    if (endsIn('b', 'l', 'e')) {
      if (!((word.charAt(j) == 'a') || (word.charAt(j) == 'i'))) return;
      word_char = word.charAt(j);
      word.setLength(j); /* try just removing the ending */
      k = j - 1;
      if (lookup()) return;
      if (doubleC(k)) { /* allow for a doubled consonant */
        word.setLength(k);
        k--;
        if (lookup()) return;
        k++;
        word.unsafeWrite(word.charAt(k - 1));
      }
      word.setLength(j);
      word.unsafeWrite('e'); /* try removing -a/ible and adding -e */
      k = j;
      if (lookup()) return;
      word.setLength(j);
      word.append("ate"); /* try removing -able and adding -ate */
      /* (e.g., compensable/compensate) */
      k = j + 2;
      if (lookup()) return;
      word.setLength(j);
      word.unsafeWrite(word_char); /* restore the original values */
      word.append("ble");
      k = old_k;
      // nolookup()
    }
    return;
  }
  
  /*
   * handle -ic endings. This is fairly straightforward, but this is also the
   * only place we try *expanding* an ending, -ic -> -ical. This is to handle
   * cases like `canonic' -> `canonical'
   */
  private void icEndings() {
    if (endsIn('i', 'c')) {
      word.setLength(j + 3);
      word.append("al"); /* try converting -ic to -ical */
      k = j + 4;
      if (lookup()) return;
      
      word.setCharAt(j + 1, 'y'); /* try converting -ic to -y */
      word.setLength(j + 2);
      k = j + 1;
      if (lookup()) return;
      
      word.setCharAt(j + 1, 'e'); /* try converting -ic to -e */
      if (lookup()) return;
      
      word.setLength(j + 1); /* try removing -ic altogether */
      k = j;
      if (lookup()) return;
      word.append("ic"); /* restore the original ending */
      k = j + 2;
      // nolookup()
    }
    return;
  }
  
  private static char[] ization = "ization".toCharArray();
  private static char[] ition = "ition".toCharArray();
  private static char[] ation = "ation".toCharArray();
  private static char[] ication = "ication".toCharArray();
  
  /* handle some derivational endings */
  /*
   * this routine deals with -ion, -ition, -ation, -ization, and -ication. The
   * -ization ending is always converted to -ize
   */
  private void ionEndings() {
    int old_k = k;
    if (!endsIn('i', 'o', 'n')) {
      return;
    }
    
    if (endsIn(ization)) { /*
                            * the -ize ending is very productive, so simply
                            * accept it as the root
                            */
      word.setLength(j + 3);
      word.unsafeWrite('e');
      k = j + 3;
      lookup();
      return;
    }
    
    if (endsIn(ition)) {
      word.setLength(j + 1);
      word.unsafeWrite('e');
      k = j + 1;
      if (lookup()) /*
                     * remove -ition and add `e', and check against the
                     * dictionary
                     */
      return; /* (e.g., definition->define, opposition->oppose) */
      
      /* restore original values */
      word.setLength(j + 1);
      word.append("ition");
      k = old_k;
      // nolookup()
    } else if (endsIn(ation)) {
      word.setLength(j + 3);
      word.unsafeWrite('e');
      k = j + 3;
      if (lookup()) /* remove -ion and add `e', and check against the dictionary */
      return; /* (elmination -> eliminate) */
      
      word.setLength(j + 1);
      word.unsafeWrite('e'); /*
                              * remove -ation and add `e', and check against the
                              * dictionary
                              */
      k = j + 1;
      if (lookup()) return;
      
      word.setLength(j + 1);/*
                             * just remove -ation (resignation->resign) and
                             * check dictionary
                             */
      k = j;
      if (lookup()) return;
      
      /* restore original values */
      word.setLength(j + 1);
      word.append("ation");
      k = old_k;
      // nolookup()
      
    }
    
    /*
     * test -ication after -ation is attempted (e.g., `complication->complicate'
     * rather than `complication->comply')
     */

    if (endsIn(ication)) {
      word.setLength(j + 1);
      word.unsafeWrite('y');
      k = j + 1;
      if (lookup()) /*
                     * remove -ication and add `y', and check against the
                     * dictionary
                     */
      return; /* (e.g., amplification -> amplify) */
      
      /* restore original values */
      word.setLength(j + 1);
      word.append("ication");
      k = old_k;
      // nolookup()
    }
    
    // if (endsIn(ion)) {
    if (true) { // we checked for this earlier... just need to set "j"
      j = k - 3; // YCS
      
      word.setLength(j + 1);
      word.unsafeWrite('e');
      k = j + 1;
      if (lookup()) /* remove -ion and add `e', and check against the dictionary */
      return;
      
      word.setLength(j + 1);
      k = j;
      if (lookup()) /* remove -ion, and if it's found, treat that as the root */
      return;
      
      /* restore original values */
      word.setLength(j + 1);
      word.append("ion");
      k = old_k;
      // nolookup()
    }
    
    // nolookup(); all of the other paths restored original values
    return;
  }
  
  /*
   * this routine deals with -er, -or, -ier, and -eer. The -izer ending is
   * always converted to -ize
   */
  private void erAndOrEndings() {
    int old_k = k;
    
    if (word.charAt(k) != 'r') return; // YCS
    
    char word_char; /* so we can remember if it was -er or -or */
    
    if (endsIn('i', 'z', 'e', 'r')) { /*
                                       * -ize is very productive, so accept it
                                       * as the root
                                       */
      word.setLength(j + 4);
      k = j + 3;
      lookup();
      return;
    }
    
    if (endsIn('e', 'r') || endsIn('o', 'r')) {
      word_char = word.charAt(j + 1);
      if (doubleC(j)) {
        word.setLength(j);
        k = j - 1;
        if (lookup()) return;
        word.unsafeWrite(word.charAt(j - 1)); /* restore the doubled consonant */
      }
      
      if (word.charAt(j) == 'i') { /* do we have a -ier ending? */
        word.setCharAt(j, 'y');
        word.setLength(j + 1);
        k = j;
        if (lookup()) /* yes, so check against the dictionary */
        return;
        word.setCharAt(j, 'i'); /* restore the endings */
        word.unsafeWrite('e');
      }
      
      if (word.charAt(j) == 'e') { /* handle -eer */
        word.setLength(j);
        k = j - 1;
        if (lookup()) return;
        word.unsafeWrite('e');
      }
      
      word.setLength(j + 2); /* remove the -r ending */
      k = j + 1;
      if (lookup()) return;
      word.setLength(j + 1); /* try removing -er/-or */
      k = j;
      if (lookup()) return;
      word.unsafeWrite('e'); /* try removing -or and adding -e */
      k = j + 1;
      if (lookup()) return;
      word.setLength(j + 1);
      word.unsafeWrite(word_char);
      word.unsafeWrite('r'); /* restore the word to the way it was */
      k = old_k;
      // nolookup()
    }
    
  }
  
  /*
   * this routine deals with -ly endings. The -ally ending is always converted
   * to -al Sometimes this will temporarily leave us with a non-word (e.g.,
   * heuristically maps to heuristical), but then the -al is removed in the next
   * step.
   */
  private void lyEndings() {
    int old_k = k;
    
    if (endsIn('l', 'y')) {
      
      word.setCharAt(j + 2, 'e'); /* try converting -ly to -le */
      
      if (lookup()) return;
      word.setCharAt(j + 2, 'y');
      
      word.setLength(j + 1); /* try just removing the -ly */
      k = j;
      
      if (lookup()) return;
      
      if ((j > 0) && (word.charAt(j - 1) == 'a') && (word.charAt(j) == 'l')) /*
                                                                              * always
                                                                              * convert
                                                                              * -
                                                                              * ally
                                                                              * to
                                                                              * -
                                                                              * al
                                                                              */
      return;
      word.append("ly");
      k = old_k;
      
      if ((j > 0) && (word.charAt(j - 1) == 'a') && (word.charAt(j) == 'b')) { /*
                                                                                * always
                                                                                * convert
                                                                                * -
                                                                                * ably
                                                                                * to
                                                                                * -
                                                                                * able
                                                                                */
        word.setCharAt(j + 2, 'e');
        k = j + 2;
        return;
      }
      
      if (word.charAt(j) == 'i') { /* e.g., militarily -> military */
        word.setLength(j);
        word.unsafeWrite('y');
        k = j;
        if (lookup()) return;
        word.setLength(j);
        word.append("ily");
        k = old_k;
      }
      
      word.setLength(j + 1); /* the default is to remove -ly */
      
      k = j;
      // nolookup()... we already tried removing the "ly" variant
    }
    return;
  }
  
  /*
   * this routine deals with -al endings. Some of the endings from the previous
   * routine are finished up here.
   */
  private void alEndings() {
    int old_k = k;
    
    if (word.length() < 4) return;
    if (endsIn('a', 'l')) {
      word.setLength(j + 1);
      k = j;
      if (lookup()) /* try just removing the -al */
      return;
      
      if (doubleC(j)) { /* allow for a doubled consonant */
        word.setLength(j);
        k = j - 1;
        if (lookup()) return;
        word.unsafeWrite(word.charAt(j - 1));
      }
      
      word.setLength(j + 1);
      word.unsafeWrite('e'); /* try removing the -al and adding -e */
      k = j + 1;
      if (lookup()) return;
      
      word.setLength(j + 1);
      word.append("um"); /* try converting -al to -um */
      /* (e.g., optimal - > optimum ) */
      k = j + 2;
      if (lookup()) return;
      
      word.setLength(j + 1);
      word.append("al"); /* restore the ending to the way it was */
      k = old_k;
      
      if ((j > 0) && (word.charAt(j - 1) == 'i') && (word.charAt(j) == 'c')) {
        word.setLength(j - 1); /* try removing -ical */
        k = j - 2;
        if (lookup()) return;
        
        word.setLength(j - 1);
        word.unsafeWrite('y');/* try turning -ical to -y (e.g., bibliographical) */
        k = j - 1;
        if (lookup()) return;
        
        word.setLength(j - 1);
        word.append("ic"); /* the default is to convert -ical to -ic */
        k = j;
        // nolookup() ... converting ical to ic means removing "al" which we
        // already tried
        // ERROR
        lookup();
        return;
      }
      
      if (word.charAt(j) == 'i') { /* sometimes -ial endings should be removed */
        word.setLength(j); /* (sometimes it gets turned into -y, but we */
        k = j - 1; /* aren't dealing with that case for now) */
        if (lookup()) return;
        word.append("ial");
        k = old_k;
        lookup();
      }
      
    }
    return;
  }
  
  /*
   * this routine deals with -ive endings. It normalizes some of the -ative
   * endings directly, and also maps some -ive endings to -ion.
   */
  private void iveEndings() {
    int old_k = k;
    
    if (endsIn('i', 'v', 'e')) {
      word.setLength(j + 1); /* try removing -ive entirely */
      k = j;
      if (lookup()) return;
      
      word.unsafeWrite('e'); /* try removing -ive and adding -e */
      k = j + 1;
      if (lookup()) return;
      word.setLength(j + 1);
      word.append("ive");
      if ((j > 0) && (word.charAt(j - 1) == 'a') && (word.charAt(j) == 't')) {
        word.setCharAt(j - 1, 'e'); /* try removing -ative and adding -e */
        word.setLength(j); /* (e.g., determinative -> determine) */
        k = j - 1;
        if (lookup()) return;
        word.setLength(j - 1); /* try just removing -ative */
        if (lookup()) return;
        
        word.append("ative");
        k = old_k;
      }
      
      /* try mapping -ive to -ion (e.g., injunctive/injunction) */
      word.setCharAt(j + 2, 'o');
      word.setCharAt(j + 3, 'n');
      if (lookup()) return;
      
      word.setCharAt(j + 2, 'v'); /* restore the original values */
      word.setCharAt(j + 3, 'e');
      k = old_k;
      // nolookup()
    }
    return;
  }
  
  KStemmer() {}
  
  String stem(String term) {
    boolean changed = stem(term.toCharArray(), term.length());
    if (!changed) return term;
    return asString();
  }
  
  /**
   * Returns the result of the stem (assuming the word was changed) as a String.
   */
  String asString() {
    String s = getString();
    if (s != null) return s;
    return word.toString();
  }
  
  CharSequence asCharSequence() {
    return result != null ? result : word;
  }
  
  String getString() {
    return result;
  }
  
  char[] getChars() {
    return word.getArray();
  }
  
  int getLength() {
    return word.length();
  }
  
  String result;
  
  private boolean matched() {
    /***
     * if (!lookups.contains(word.toString())) { throw new
     * RuntimeException("didn't look up "+word.toString()+" prev="+prevLookup);
     * }
     ***/
    // lookup();
    return matchedEntry != null;
  }
  
  /**
   * Stems the text in the token. Returns true if changed.
   */
  boolean stem(char[] term, int len) {
    
    result = null;
    
    k = len - 1;
    if ((k <= 1) || (k >= MaxWordLen - 1)) {
      return false; // don't stem
    }
    
    // first check the stemmer dictionaries, and avoid using the
    // cache if it's in there.
    DictEntry entry = dict_ht.get(term, 0, len);
    if (entry != null) {
      if (entry.root != null) {
        result = entry.root;
        return true;
      }
      return false;
    }
    
    /***
     * caching off is normally faster if (cache == null) initializeStemHash();
     * 
     * // now check the cache, before we copy chars to "word" if (cache != null)
     * { String val = cache.get(term, 0, len); if (val != null) { if (val !=
     * SAME) { result = val; return true; } return false; } }
     ***/
    
    word.reset();
    // allocate enough space so that an expansion is never needed
    word.reserve(len + 10);
    for (int i = 0; i < len; i++) {
      char ch = term[i];
      if (!isAlpha(ch)) return false; // don't stem
      // don't lowercase... it's a requirement that lowercase filter be
      // used before this stemmer.
      word.unsafeWrite(ch);
    }
    
    matchedEntry = null;
    /***
     * lookups.clear(); lookups.add(word.toString());
     ***/
    
    /*
     * This while loop will never be executed more than one time; it is here
     * only to allow the break statement to be used to escape as soon as a word
     * is recognized
     */
    while (true) {
      // YCS: extra lookup()s were inserted so we don't need to
      // do an extra wordInDict() here.
      plural();
      if (matched()) break;
      pastTense();
      if (matched()) break;
      aspect();
      if (matched()) break;
      ityEndings();
      if (matched()) break;
      nessEndings();
      if (matched()) break;
      ionEndings();
      if (matched()) break;
      erAndOrEndings();
      if (matched()) break;
      lyEndings();
      if (matched()) break;
      alEndings();
      if (matched()) break;
      entry = wordInDict();
      iveEndings();
      if (matched()) break;
      izeEndings();
      if (matched()) break;
      mentEndings();
      if (matched()) break;
      bleEndings();
      if (matched()) break;
      ismEndings();
      if (matched()) break;
      icEndings();
      if (matched()) break;
      ncyEndings();
      if (matched()) break;
      nceEndings();
      matched();
      break;
    }
    
    /*
     * try for a direct mapping (allows for cases like `Italian'->`Italy' and
     * `Italians'->`Italy')
     */
    entry = matchedEntry;
    if (entry != null) {
      result = entry.root; // may be null, which means that "word" is the stem
    }
    
    /***
     * caching off is normally faster if (cache != null && cache.size() <
     * maxCacheSize) { char[] key = new char[len]; System.arraycopy(term, 0,
     * key, 0, len); if (result != null) { cache.put(key, result); } else {
     * cache.put(key, word.toString()); } }
     ***/
    
    /***
     * if (entry == null) { if (!word.toString().equals(new String(term,0,len)))
     * { System.out.println("CASE:" + word.toString() + "," + new
     * String(term,0,len));
     * 
     * } }
     ***/
    
    // no entry matched means result is "word"
    return true;
  }
  
}