/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.ml.inference;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@State(Scope.Benchmark)
public class LangIdentBenchmark {

    TrainedModelDefinition landIdentModel;
    ClassificationConfig classificationConfig;
    NamedXContentRegistry xContentRegistry;

    @Setup
    public void setup() throws IOException {
        classificationConfig = new ClassificationConfig(1);
        xContentRegistry = xContentRegistry();
        landIdentModel = loadModelFromResource();
    }

    @Benchmark
    public InferenceResults inferBn() {
        Map<String, Object> doc = new HashMap<>();
        doc.put("text", "গ্যালারির ৩৮ বছর পূর্তিতে মূল্যছাড় অর্থনীতি বিএনপির ওয়াক আউট তপন চৌধুরী হারবাল অ্যাসোসিয়েশনের " +
                "সভাপতি আন্তর্জাতিক পরামর্শক বোর্ড দিয়ে শরিয়াহ্ ইনন্ডেক্স করবে সিএসই মালিকপক্ষের কান্না, শ্রমিকের অনিশ্চয়তা " +
                "মতিঝিলে সমাবেশ নিষিদ্ধ: এফবিসিসিআইয়ের ধন্যবাদ বিনোদন বিশেষ প্রতিবেদন বাংলালিংকের গ্র্যান্ডমাস্টার সিজন-৩ ব্রাজিলে " +
                "বিশ্বকাপ ফুটবল আয়োজনবিরোধী বিক্ষোভ দেশের " +
                "নিরাপত্তার চেয়ে অনেক বেশি সচেতন । প্রার্থীদের দক্ষতা  ও যোগ্যতার পাশাপাশি তারা জাতীয় " +
                "ইস্যুগুলোতে প্রাধান্য দিয়েছেন । ” " +
                "পাঁচটি সিটিতে ২০ লাখ ভোটারদের দিয়ে জাতীয় নির্বাচনে ৮ কোটি ভোটারদের সঙ্গে তুলনা করা যাবে " +
                "কি একজন দর্শকের এমন প্রশ্নে জবাবে আব্দুল্লাহ " +
                "আল নোমান বলেন , “ এই পাঁচটি সিটি কর্পোরেশন নির্বাচন দেশের পাঁচটি বড় বিভাগের প্রতিনিধিত্ব করছে । " +
                "এছাড়া এখানকার ভোটার রা সবাই সচেতন । তারা");
        return landIdentModel.infer(doc, classificationConfig);
    }

    @Benchmark
    public InferenceResults inferCa() {
        Map<String, Object> doc = new HashMap<>();
        doc.put("text", "al final en un únic lloc nhorabona l correu electrònic està concebut " +
                "com a eina de productivitat aleshores per què perdre el temps arxivant missatges per després intentar " +
                "recordar on els veu desar i per què heu d eliminar missatges importants per l");
        return landIdentModel.infer(doc, classificationConfig);
    }

    @Benchmark
    public InferenceResults inferCy() {
        Map<String, Object> doc = new HashMap<>();
        doc.put("text", " a chofrestru eich cyfrif ymwelwch a unwaith i chi greu eich cyfrif mi fydd yn " +
                "cael ei hysbysu o ch cyfeiriad ebost newydd fel eich bod yn gallu cadw mewn cysylltiad drwy gmail os nad ydych " +
                "chi wedi clywed yn barod am gmail mae n gwasanaeth gwebost");
        return landIdentModel.infer(doc, classificationConfig);
    }

    @Benchmark
    public InferenceResults inferEl() {
        Map<String, Object> doc = new HashMap<>();
        doc.put("text", " ή αρνητική αναζήτηση λέξης κλειδιού καταστήστε τις μεμονωμένες" +
                " λέξεις κλειδιά περισσότερο στοχοθετημένες με τη μετατροπή τους σε");
        return landIdentModel.infer(doc, classificationConfig);
    }

    @Benchmark
    public InferenceResults inferEs() {
        Map<String, Object> doc = new HashMap<>();
        doc.put("text", " a continuación haz clic en el botón obtener ruta también puedes desplazarte hasta el final de la página " +
                "para cambiar tus opciones de búsqueda gráfico y detalles ésta es una lista de los vídeos que te recomendamos " +
                "nuestras recomendaciones se basan");
        return landIdentModel.infer(doc, classificationConfig);
    }

    @Benchmark
    public InferenceResults inferFa() {
        Map<String, Object> doc = new HashMap<>();
        doc.put("text", " آب خوردن عجله می کردند به جای باز ی کتک کاری " +
                "می کردند و همه چيز مثل قبل بود فقط من ماندم و يک دنيا حرف و انتظار تا عاقبت رسيد احضاريه ی ای با");
        return landIdentModel.infer(doc, classificationConfig);
    }

    @Benchmark
    public InferenceResults inferHi() {
        Map<String, Object> doc = new HashMap<>();
        doc.put("text", " ak pitit tout sosyete a chita se pou sa leta dwe pwoteje yo nimewo leta fèt pou li pwoteje tout " +
                "paran ak pitit nan peyi a menm jan kit paran yo marye kit yo pa marye tout manman ki fè pitit leta fèt " +
                "pou ba yo konkoul menm jan tou pou timoun piti ak pou");
        return landIdentModel.infer(doc, classificationConfig);
    }

    @Benchmark
    public InferenceResults inferMk() {
        Map<String, Object> doc = new HashMap<>();
        doc.put("text", " гласовите коалицијата на вмро дпмне како партија со најмногу освоени гласови ќе " +
                "добие евра а на сметката на коализијата за македонија");
        return landIdentModel.infer(doc, classificationConfig);
    }

    @Benchmark
    public InferenceResults inferNl() {
        Map<String, Object> doc = new HashMap<>();
        doc.put("text",  "a als volgt te werk om een configuratiebestand te maken sitemap gen py ebruik filters om de s op te " +
                "geven die moeten worden toegevoegd of uitgesloten op basis van de opmaaktaal elke sitemap mag alleen de s " +
                "bevatten voor een bepaalde opmaaktaal dit");
        return landIdentModel.infer(doc, classificationConfig);
    }

    @Benchmark
    public InferenceResults inferTr() {
        Map<String, Object> doc = new HashMap<>();
        doc.put("text", " a ayarlarınızı görmeniz ve yönetmeniz içindir eğer kampanyanız için günlük bütçenizi gözden " +
                "geçirebileceğiniz yeri arıyorsanız kampanya yönetimi ne gidin kampanyanızı seçin ve kampanya ayarlarını " +
                "düzenle yi tıklayın sunumu");
        return landIdentModel.infer(doc, classificationConfig);
    }

    private NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        return new NamedXContentRegistry(namedXContent);
    }

    private TrainedModelDefinition loadModelFromResource() throws IOException {
        String path = TrainedModelProvider.MODEL_RESOURCE_PATH + "lang_ident_model_1.json";
        URL resource = getClass().getResource(path);
        if (resource == null) {
            throw new ResourceNotFoundException("Lang ident model not found: " + path);
        }
        BytesReference bytes = Streams.readFully(getClass()
                .getResourceAsStream(path));
        try (XContentParser parser =
                     XContentHelper.createParser(xContentRegistry,
                             LoggingDeprecationHandler.INSTANCE,
                             bytes,
                             XContentType.JSON)) {
            TrainedModelConfig.Builder builder = TrainedModelConfig.fromXContent(parser, true);
            return builder.build().ensureParsedDefinition(xContentRegistry).getModelDefinition();
        }
    }
}
