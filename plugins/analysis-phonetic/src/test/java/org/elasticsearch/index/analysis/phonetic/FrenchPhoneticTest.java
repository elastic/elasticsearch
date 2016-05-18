package org.elasticsearch.index.analysis.phonetic;

import org.apache.commons.codec.EncoderException;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyOrNullString;

public class FrenchPhoneticTest {

    @Test
    public void testEncodeWithMutedContainsH() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("thermometre");
        MatcherAssert.assertThat(encode, equalTo("T2RMOM2TR"));
    }

    @Test
    public void testEncodeNumber() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("5");
        MatcherAssert.assertThat(encode, equalTo(""));
    }

    @Test
    public void testEncodeWithFinalX() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("CEDEX");
        MatcherAssert.assertThat(encode, equalTo("S2D2X"));
    }

    @Test
    public void testEncodeWithStartingOEU() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("oeuvre");
        MatcherAssert.assertThat(encode, equalTo("8VR"));
    }

    @Test
    public void testEncodeWithContainsOEU() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("coeur");
        MatcherAssert.assertThat(encode, equalTo("K8R"));
    }

    @Test
    public void testEncodeWithEndingOEU() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("voeu");
        MatcherAssert.assertThat(encode, equalTo("V8"));
    }

    @Test
    public void testEncodeWithContainsEU() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("veuve");
        MatcherAssert.assertThat(encode, equalTo("V8V"));
    }

    @Test
    public void testEncodeWithEndingsEU() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("matthieu");
        MatcherAssert.assertThat(encode, equalTo("MATI8"));
    }

    @Test
    public void testEncodeWithStartingEU() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("europe");
        MatcherAssert.assertThat(encode, equalTo("8ROP"));
    }

    @Test
    public void testEncodeWithMutedContainsHNotPreceded() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("hache");
        MatcherAssert.assertThat(encode, equalTo("ACH"));
    }

    @Test
    public void testEncodeWithMutedContainsHPrecedeByVowels() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("bahut");
        MatcherAssert.assertThat(encode, equalTo("BAU"));
    }

    @Test
    public void testEncodeWithMutedStartingH() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("haricots");
        MatcherAssert.assertThat(encode, equalTo("ARIKO"));
    }

    @Test
    public void testEncodeWithMutedEndingX() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("hiboux");
        MatcherAssert.assertThat(encode, equalTo("IBOU"));
    }

    @Test
    public void testEncodeWithMutedEndingTS() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("couts");
        MatcherAssert.assertThat(encode, equalTo("KOU"));
    }

    @Test
    public void testEncodeWithTrailingT() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("rat");
        MatcherAssert.assertThat(encode, equalTo("RA"));
    }

    @Test
    public void testEncodeWithTrailingY() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("dauby");
        MatcherAssert.assertThat(encode, equalTo("DOBI"));
    }

    @Test
    public void testEncodeWithTwoTrailingMutedConsonnant() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("ouest");
        MatcherAssert.assertThat(encode, equalTo("OU"));
    }

    @Test
    public void testEncodeWithTrailingD() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("suspend");
        MatcherAssert.assertThat(encode, equalTo("SUSP3"));
    }

    @Test
    public void testEncodeWithNotMutedH() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("chat");
        MatcherAssert.assertThat(encode, equalTo("CHA"));
    }

    @Test
    public void testEncodeWithDoubleCH() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("cherche");
        MatcherAssert.assertThat(encode, equalTo("CH2RCH"));
    }

    @Test
    public void testEncodeWithCAsS() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("acceptable");
        MatcherAssert.assertThat(encode, equalTo("AKS2PTABL"));
    }

    @Test
    public void testEncodeWithCAsK() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("consommable");
        MatcherAssert.assertThat(encode, equalTo("K4SOMABL"));
    }

    @Test
    public void testEncodeWithDoubleConsonant() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("boulette");
        MatcherAssert.assertThat(encode, equalTo("BOUL2T"));
    }

    @Test
    public void testEncodeWithQUAsK() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("graphique");
        MatcherAssert.assertThat(encode, equalTo("GRAFIK"));
    }

    @Test
    public void testEncodeWithTrailingR() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("merard");
        MatcherAssert.assertThat(encode, equalTo("M2RAR"));
    }

    @Test
    public void testEncodeWithQUAsKNotEnds() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("graphiques");
        MatcherAssert.assertThat(encode, equalTo("GRAFIK"));
    }

    @Test
    public void testEncodeWithQUAsKEnds() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("graphiqu");
        MatcherAssert.assertThat(encode, equalTo("GRAFIK"));
    }

    @Test
    public void testEncodeWithPHAlmostEnding() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("graphe");
        MatcherAssert.assertThat(encode, equalTo("GRAF"));
    }

    @Test
    public void testEncodeWithPHEnding() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("graph");
        MatcherAssert.assertThat(encode, equalTo("GRAF"));
    }


    @Test
    public void testEncodeWithAIEnding() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("balai");
        MatcherAssert.assertThat(encode, equalTo("BAL2"));
    }

    @Test
    public void testEncodeWithCEnding() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("FRANC");
        MatcherAssert.assertThat(encode, equalTo("FR3"));
    }

    @Test
    public void testEncodeWithCKEnding() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("FRANCK");
        MatcherAssert.assertThat(encode, equalTo("FR3K"));
    }

    @Test
    public void testEncodeWithKEnding() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("FRANK");
        MatcherAssert.assertThat(encode, equalTo("FR3K"));
    }

    @Test
    public void testEncodeWithFDSQFSFSDFEnding() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("BRAISE");
        MatcherAssert.assertThat(encode, equalTo("BR2Z"));
    }

    @Test
    public void testEncodeWithAYEnding() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("tramway");
        MatcherAssert.assertThat(encode, equalTo("TR3V2"));
    }

    @Test
    public void testEncodeWithANEnding() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("faisan");
        MatcherAssert.assertThat(encode, equalTo("F2Z3"));
    }

    @Test
    public void testEncodeWithGEEnding() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("tige");
        MatcherAssert.assertThat(encode, equalTo("TIJ"));
    }

    @Test
    public void testEncodeWithAIMEnding() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("daim");
        MatcherAssert.assertThat(encode, equalTo("D1"));
    }

    @Test
    public void testEncodeWithAINEnding() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("bain");
        MatcherAssert.assertThat(encode, equalTo("B1"));
    }

    @Test
    public void testEncodeWithEINEnding() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("plein");
        MatcherAssert.assertThat(encode, equalTo("PL1"));
    }

    @Test
    public void testEncodeWithEINContains() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("ceinture");
        MatcherAssert.assertThat(encode, equalTo("S1TUR"));
    }

    @Test
    public void testEncodeWithEINContainsFollowByVowels() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("seime");
        MatcherAssert.assertThat(encode, equalTo("S2M"));
    }

    @Test
    public void testEncodeWithAINContains() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("maintenant");
        MatcherAssert.assertThat(encode, equalTo("M1TEN3"));
    }

    @Test
    public void testEncodeWithAINAlmostEnding() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("bains");
        MatcherAssert.assertThat(encode, equalTo("B1"));
    }

    @Test
    public void testEncodeWithEY() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("seyante");
        MatcherAssert.assertThat(encode, equalTo("S2I3T"));
    }

    @Test
    public void testEncodeWithTrailingEY() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("volley");
        MatcherAssert.assertThat(encode, equalTo("VOL2"));
    }

    @Test
    public void testEncodeWithEndingEY() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("chardonay");
        MatcherAssert.assertThat(encode, equalTo("CHARDON2"));
    }

    @Test
    public void testEncodeWithTrailingET() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.encode("charret");
        MatcherAssert.assertThat(result, equalTo("CHAR2"));
    }

    @Test
    public void testEncodeWithTrailingAU() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.encode("tuyau");
        MatcherAssert.assertThat(result, equalTo("TUIO"));
    }

    @Test
    public void testEncodeWithTrailingYAndMutedConsonant() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.encode("pays");
        MatcherAssert.assertThat(result, equalTo("P2I"));
    }

    @Test
    public void testEncodeWithAYContains() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.encode("payer");
        MatcherAssert.assertThat(result, equalTo("P2I2"));
    }

    @Test
    public void testEncodeWithTrailingETLongWord() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.encode("trajet");
        MatcherAssert.assertThat(result, equalTo("TRAJ2"));
    }

    @Test
    public void testEncodeWithTrailingER() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.encode("arriver");
        MatcherAssert.assertThat(result, equalTo("ARIV2"));
    }

    @Test
    public void testEncodeWithTrailingE() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.encode("vraie");
        MatcherAssert.assertThat(result, equalTo("VR2"));
    }

    @Test
    public void testEncodeWithON() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("pont");
        MatcherAssert.assertThat(encode, equalTo("P4"));
    }

    @Test
    public void testEncodeWithOM() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("pompe");
        MatcherAssert.assertThat(encode, equalTo("P4P"));
    }

    @Test
    public void testEncodeWithYM() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("cymbale");
        MatcherAssert.assertThat(encode, equalTo("S1BAL"));
    }

    @Test
    public void testEncodeWithYN() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("pharynx");
        MatcherAssert.assertThat(encode, equalTo("FAR1X"));
    }

    @Test
    public void testEncodeWithAN() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("pendant");
        MatcherAssert.assertThat(encode, equalTo("P3D3"));
    }

    @Test
    public void testEncodeWithEndingEAU() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("eau");
        MatcherAssert.assertThat(encode, equalTo("O"));
    }

    @Test
    public void testEncodeWithEAUContains() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("tableautage");
        MatcherAssert.assertThat(encode, equalTo("TABLOTAJ"));
    }

    @Test
    public void testEncodeWithAUContains() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("vautrer");
        MatcherAssert.assertThat(encode, equalTo("VOTR2"));
    }

    @Test
    public void testEncodeWithANFollowByVowels() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("anhile");
        MatcherAssert.assertThat(encode, equalTo("ANIL"));
    }

    @Test
    public void testEncodeWithAMFollowByVowels() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("amerique");
        MatcherAssert.assertThat(encode, equalTo("AM2RIK"));
    }

    @Test
    public void testEncodeWithVowelTION() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("ebulition");
        MatcherAssert.assertThat(encode, equalTo("2BULISI4"));
    }

    @Test
    public void testEncodeWithConsonantTION() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("bastion");
        MatcherAssert.assertThat(encode, equalTo("BASTI4"));
    }

    @Test
    public void testEncodeWithEN() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("pendant");
        MatcherAssert.assertThat(encode, equalTo("P3D3"));
    }

    @Test
    public void testEncodeWithNullString() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("");
        MatcherAssert.assertThat(encode, equalTo(""));
    }

    @Test
    public void testSubstringStartIndexGreaterThanLengthExpectedEmptyString() throws Exception {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.substring("E", 2, 4);
        MatcherAssert.assertThat(result, equalTo(""));
    }
    @Test
    public void testSubstringStartIndexEqualsToLengthExpectedEmptyString() throws Exception {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.substring("E", 1, 4);
        MatcherAssert.assertThat(result, equalTo(""));
    }

    @Test
    public void testSubstringStartIndexGreaterThanLengthExpectedNull() throws Exception {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.substring("E", 2, 4);
        MatcherAssert.assertThat(result, isEmptyOrNullString());
    }

    @Test
    public void testSubstringStartIndexEqualsToLengthExpectedNull() throws Exception {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.substring("E", 1, 4);
        MatcherAssert.assertThat(result, isEmptyOrNullString());
    }

    @Test
    public void testSubstringEndIndexEqualsToTailLength() throws Exception {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.substring("E", 1, 1);
        MatcherAssert.assertThat(result, isEmptyOrNullString());
    }

    @Test
    public void testSubstringEndIndexGreaterToTailLength() throws Exception {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.substring("EE", 0, 2);
        MatcherAssert.assertThat(result, equalTo("EE"));
    }

    @Test
    public void testSubstringEndIndexEqualsToStartIndex() throws Exception {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.substring("ERT", 2, 2);
        MatcherAssert.assertThat(result, equalTo(""));
    }

    @Test
    public void testSubstringEndIndexLesserThanLength() throws Exception {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.substring("ERT", 1, 2);
        MatcherAssert.assertThat(result, equalTo("R"));
    }

    @Test
    public void testSubstringEndIndexEqualsToLength() throws Exception {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String result = frenchPhonetic.substring("ERT", 1, 3);
        MatcherAssert.assertThat(result, equalTo("RT"));
    }
    @Test
    public void testEndingERT() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("AUBERT");
        MatcherAssert.assertThat(encode, equalTo("OB2"));
    }

    @Test
    public void testEndingER() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("OBER");
        MatcherAssert.assertThat(encode, equalTo("OB2"));
    }
    @Test
    public void testBeginningER() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("HERTZIEN");
        MatcherAssert.assertThat(encode, equalTo("2RTZI3"));
    }
    @Test
    public void testEndindAUD() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("BADAUDS");
        MatcherAssert.assertThat(encode, equalTo("BADO"));
    }
    @Test
    public void testEndingCCO() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("BACCO");
        MatcherAssert.assertThat(encode,equalTo("BAKO"));
    }

    @Test
    public void testEndingCO() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("BACO");
        MatcherAssert.assertThat(encode, equalTo("BAKO"));
    }
    @Test
    public void testEndingCOT() throws EncoderException {
        FrenchPhonetic frenchPhonetic = new FrenchPhonetic();
        String encode = frenchPhonetic.encode("BACOT");
        MatcherAssert.assertThat(encode, equalTo("BAKO"));
    }
}
