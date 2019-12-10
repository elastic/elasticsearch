/* Copyright 2016 Google Inc. All Rights Reserved. */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident;

// Set of examples from google cld3 code
public final class LanguageExamples {

    private LanguageExamples() {}

    public static final String kTestStrAF =
        "Dit is 'n kort stukkie van die teks wat gebruik sal word vir die toets " +
            "van die akkuraatheid van die nuwe benadering.";

    public static final String kTestStrAR = "احتيالية بيع أي حساب";

    public static final String kTestStrAZ =
        " a az qalıb breyn rinq intellektual oyunu üzrə yarışın zona mərhələləri " +
            "keçirilib miq un qalıqlarının dənizdən çıxarılması davam edir məhəmməd " +
            "peyğəmbərin karikaturalarını çap edən qəzetin baş redaktoru iş otağında " +
            "ölüb";

    public static final String kTestStrBE =
        " а друкаваць іх не было тэхнічна магчыма бліжэй за вільню тым самым часам " +
            "нямецкае кіраўніцтва прапаноўвала апроч ўвядзення лацінкі яе";

    public static final String kTestStrBG =
        " а дума попада в състояние на изпитание ключовите думи с предсказана " +
            "малко под то изискване на страниците за търсене в";

    public static final String kTestStrBN =
        "গ্যালারির ৩৮ বছর পূর্তিতে মূল্যছাড় অর্থনীতি বিএনপির ওয়াক আউট তপন" +
            " চৌধুরী হারবাল অ্যাসোসিয়েশনের সভাপতি আন্তর্জাতিক পরামর্শক " +
            "বোর্ড দিয়ে শরিয়াহ্ ইনন্ডেক্স করবে " +
            "সিএসই মালিকপক্ষের কান্না, শ্রমিকের অনিশ্চয়তা মতিঝিলে সমাবেশ নিষিদ্ধ: " +
            "এফবিসিসিআইয়ের ধন্যবাদ বিনোদন বিশেষ প্রতিবেদন বাংলালিংকের গ্র্যান্ডমাস্টার " +
            "সিজন-৩ ব্রাজিলে বিশ্বকাপ ফুটবল আয়োজনবিরোধী বিক্ষোভ দেশের নিরাপত্তার" +
            "  চেয়ে অনেক বেশি সচেতন । প্রার্থীদের দক্ষতা  ও যোগ্যতার" +
            " পাশাপাশি তারা জাতীয় ইস্যুগুলোতে প্রাধান্য দিয়েছেন । ” পাঁচটি সিটিতে ২০" +
            " লাখ ভোটারদের দিয়ে জাতীয় নির্বাচনে ৮ কোটি ভোটারদের" +
            " সঙ্গে তুলনা করা যাবে কি একজন দর্শকের এমন প্রশ্নে জবাবে আব্দুল্লাহ " +
            "আল নোমান বলেন , “ এই পাঁচটি সিটি কর্পোরেশন নির্বাচন দেশের পাঁচটি বড়" +
            " বিভাগের প্রতিনিধিত্ব করছে । এছাড়া এখানকার ভোটার রা সবাই সচেতন । তারা";

    public static final String kTestStrBS =
        "Novi predsjednik Mešihata Islamske zajednice u Srbiji (IZuS) i muftija " +
            "dr. Mevlud ef. Dudić izjavio je u intervjuu za Anadolu Agency (AA) kako " +
            "je uvjeren da će doći do vraćanja jedinstva među muslimanima i unutar " +
            "Islamske zajednice na prostoru Sandžaka, te da je njegova ruka pružena za " +
            "povratak svih u okrilje Islamske zajednice u Srbiji nakon skoro sedam " +
            "godina podjela u tom dijelu Srbije. Dudić je za predsjednika Mešihata IZ " +
            "u Srbiji izabran 4. januara, a zvanična inauguracija će biti obavljena u " +
            "prvoj polovini februara. Kako se očekuje, prisustvovat će joj i " +
            "reisu-l-ulema Islamske zajednice u Srbiji Husein ef. Kavazović koji će i " +
            "zvanično promovirati Dudića u novog prvog čovjeka IZ u Srbiji. Dudić će " +
            "danas boraviti u prvoj zvaničnoj posjeti reisu Kavazoviću, što je njegov " +
            "privi simbolični potez nakon imenovanja. ";

    public static final String kTestStrCA =
        "al final en un únic lloc nhorabona l correu electrònic està concebut com " +
            "a eina de productivitat aleshores per què perdre el temps arxivant " +
            "missatges per després intentar recordar on els veu desar i per què heu d " +
            "eliminar missatges importants per l";

    public static final String kTestStrCEB =
        "Ang Sugbo usa sa mga labing ugmad nga lalawigan sa nasod. Kini ang sentro " +
            "sa komersyo, edukasyon ug industriya sa sentral ug habagatang dapit sa " +
            "kapupod-an. Ang mipadayag sa Sugbo isip ikapito nga labing nindot nga " +
            "pulo sa , ang nag-inusarang pulo sa Pilipinas nga napasidunggan sa maong " +
            "magasin sukad pa sa tuig";

    public static final String kTestStrCS =
        " a akci opakujte film uložen vykreslit gmail tokio smazat obsah adresáře " +
            "nelze načíst systémový profil jednotky smoot okud používáte pro určení " +
            "polokoule značky z západ nebo v východ používejte nezáporné hodnoty " +
            "zeměpisné délky nelze";

    public static final String kTestStrCY =
        " a chofrestru eich cyfrif ymwelwch a unwaith i chi greu eich cyfrif mi " +
            "fydd yn cael ei hysbysu o ch cyfeiriad ebost newydd fel eich bod yn gallu " +
            "cadw mewn cysylltiad drwy gmail os nad ydych chi wedi clywed yn barod am " +
            "gmail mae n gwasanaeth gwebost";

    public static final String kTestStrDA =
        " a z tallene og punktummer der er tilladte log ud angiv den ønskede " +
            "adgangskode igen november gem personlige oplysninger kontrolspørgsmål det " +
            "sidste tegn i dit brugernavn skal være et bogstav a z eller tal skriv de " +
            "tegn du kan se i billedet nedenfor";

    public static final String kTestStrDE =
        " abschnitt ordner aktivieren werden die ordnereinstellungen im " +
            "farbabschnitt deaktiviert öchten sie wirklich fortfahren eldtypen angeben " +
            "optional n diesem schritt geben sie für jedesfeld aus dem datenset den " +
            "typ an ieser schritt ist optional eldtypen";

    public static final String kTestStrEL =
        " ή αρνητική αναζήτηση λέξης κλειδιού καταστήστε τις μεμονωμένες λέξεις " +
            "κλειδιά περισσότερο στοχοθετημένες με τη μετατροπή τους σε";

    public static final String kTestStrEN =
        " a backup credit card by visiting your billing preferences page or visit " +
            "the adwords help centre for more details https adwords google com support " +
            "bin answer py answer hl en we were unable to process the payment of for " +
            "your outstanding google adwords";

    public static final String kTestStrEO =
        " a jarcento refoje per enmetado de koncerna pastro tiam de reformita " +
            "konfesio ekde refoje ekzistis luteranaj komunumanoj tamen tiuj fondis " +
            "propran komunumon nur en ambaŭ apartenis ekde al la evangela eklezio en " +
            "prusio resp ties rejnlanda provinceklezio en";

    public static final String kTestStrES =
        " a continuación haz clic en el botón obtener ruta también puedes " +
            "desplazarte hasta el final de la página para cambiar tus opciones de " +
            "búsqueda gráfico y detalles ésta es una lista de los vídeos que te " +
            "recomendamos nuestras recomendaciones se basan";

    public static final String kTestStrET =
        " a niipea kui sinu maksimaalne igakuine krediidi limiit on meie poolt " +
            "heaks kiidetud on sinu kohustuseks see krediidilimiit";

    public static final String kTestStrEU =
        " a den eraso bat honen kontra hortaz eragiketa bakarrik behar dituen " +
            "eraso batek aes apurtuko luke nahiz eta oraingoz eraso bideraezina izan " +
            "gaur egungo teknologiaren mugak direla eta oraingoz kezka hauek alde " +
            "batera utzi daitezke orain arteko indar";

    public static final String kTestStrFA =
        " آب خوردن عجله می کردند به جای باز ی کتک کاری می کردند و همه چيز مثل قبل " +
            "بود فقط من ماندم و يک دنيا حرف و انتظار تا عاقبت رسيد احضاريه ی ای با";

    public static final String kTestStrFI =
        " a joilla olet käynyt tämä kerro meille kuka ä olet ei tunnistettavia " +
            "käyttötietoja kuten virheraportteja käytetään google desktopin " +
            "parantamiseen etsi näyttää mukautettuja uutisia google desktop " +
            "keskivaihto leikkaa voit kaksoisnapsauttaa";

    public static final String kTestStrFIL =
        "Ito ay isang maikling piraso ng teksto na ito ay gagamitin para sa " +
            "pagsubok ang kawastuhan ng mga bagong diskarte.";

    public static final String kTestStrFR =
        " a accès aux collections et aux frontaux qui lui ont été attribués il " +
            "peut consulter et modifier ses collections et exporter des configurations " +
            "de collection toutefois il ne peut pas créer ni supprimer des collections " +
            "enfin il a accès aux fonctions";

    public static final String kTestStrGA =
        " a bhfuil na focail go léir i do cheist le fáil orthu ní gá ach focail " +
            "breise a chur leis na cinn a cuardaíodh cheana chun an cuardach a " +
            "bheachtú nó a chúngú má chuirtear focal breise isteach aimseofar fo aicme " +
            "ar leith de na torthaí a fuarthas";

    public static final String kTestStrGL =
        "  debe ser como mínimo taranto tendas de venda polo miúdo cociñas " +
            "servizos bordado canadá viaxes parques de vehículos de recreo hotel " +
            "oriental habitación recibir unha postal no enderezo indicado " +
            "anteriormente";

    public static final String kTestStrGU =
        " આના પરિણામ પ્રમાણસર ફોન્ટ અવતરણ ચિન્હવાળા પાઠને છુપાવો બધા સમૂહો શોધાયા" +
            " હાલનો જ સંદેશ વિષયની";

    public static final String kTestStrHA =
        " a cikin a kan sakamako daga sakwannin a kan sakamako daga sakwannin daga " +
            "ranar zuwa a kan sakamako daga guda daga ranar zuwa a kan sakamako daga " +
            "shafukan daga ranar zuwa a kan sakamako daga guda a cikin last hour a kan " +
            "sakamako daga guda daga kafar";

    public static final String kTestStrHI =
        " ं ऐडवर्ड्स विज्ञापनों के अनुभव पर आधारित हैं और इनकी मदद से आपको अपने" +
            " विज्ञापनों का अधिकतम लाभ";

    public static final String kTestStrHMN =
        "Qhov no yog ib tug luv luv daim ntawv nyeem uas yuav siv tau rau kev soj " +
            "ntsuam qhov tseeb ntawm tus tshiab mus kom ze.";

    public static final String kTestStrHR =
        "Posljednja dva vladara su Kijaksar (Κυαξαρης; 625-585 prije Krista), " +
            "fraortov sin koji će proširiti teritorij Medije i Astijag. Kijaksar je " +
            "imao kćer ili unuku koja se zvala Amitis a postala je ženom " +
            "Nabukodonosora II. kojoj je ovaj izgradio Viseće vrtove Babilona. " +
            "Kijaksar je modernizirao svoju vojsku i uništio Ninivu 612. prije Krista. " +
            "Naslijedio ga je njegov sin, posljednji medijski kralj, Astijag, kojega " +
            "je detronizirao (srušio sa vlasti) njegov unuk Kir Veliki. Zemljom su " +
            "zavladali Perzijanci. Hrvatska je zemlja situacija u Europi. Ona ima " +
            "bogatu kulturu i ukusna jela.";

    public static final String kTestStrHT =
        " ak pitit tout sosyete a chita se pou sa leta dwe pwoteje yo nimewo leta " +
            "fèt pou li pwoteje tout paran ak pitit nan peyi a menm jan kit paran yo " +
            "marye kit yo pa marye tout manman ki fè pitit leta fèt pou ba yo konkoul " +
            "menm jan tou pou timoun piti ak pou";

    public static final String kTestStrHU =
        " a felhasználóim a google azonosító szöveget ikor látják a felhasználóim " +
            "a google azonosító szöveget felhasználók a google azonosító szöveget " +
            "fogják látni minden tranzakció után ha a vásárlását regisztrációját " +
            "oldalunk";

    public static final String kTestStrHY =
        " ա յ եվ նա հիացած աչքերով նայում է հինգհարկանի շենքի տարօրինակ փոքրիկ " +
            "քառակուսի պատուհաններին դեռ մենք շատ ենք հետամնաց ասում է նա այսպես է";

    public static final String kTestStrID =
        "berdiri setelah pengurusnya yang berusia 83 tahun, Fayzrahman Satarov, " +
            "mendeklarasikan diri sebagai nabi dan rumahnya sebagai negara Islam " +
            "Satarov digambarkan sebagai mantan ulama Islam  tahun 1970-an. " +
            "Pengikutnya didorong membaca manuskripnya dan kebanyakan dilarang " +
            "meninggalkan tempat persembunyian bawah tanah di dasar gedung delapan " +
            "lantai mereka. Jaksa membuka penyelidikan kasus kriminal pada kelompok " +
            "itu dan menyatakan akan membubarkan kelompok kalau tetap melakukan " +
            "kegiatan ilegal seperti mencegah anggotanya mencari bantuan medis atau " +
            "pendidikan. Sampai sekarang pihak berwajib belum melakukan penangkapan " +
            "meskipun polisi mencurigai adanya tindak kekerasan pada anak. Pengadilan " +
            "selanjutnya akan memutuskan apakah anak-anak diizinkan tetap tinggal " +
            "dengan orang tua mereka. Kazan yang berada sekitar 800 kilometer di timur " +
            "Moskow merupakan wilayah Tatarstan yang";

    public static final String kTestStrIG =
        "Chineke bụ aha ọzọ ndï omenala Igbo kpọro Chukwu. Mgbe ndị bekee bịara, " +
            "ha mee ya nke ndi Christian. N'echiche ndi ekpere chi Omenala Ndi Igbo, " +
            "Christianity, Judaism, ma Islam, Chineke nwere ọtụtụ utu aha, ma nwee " +
            "nanị otu aha. Ụzọ abụọ e si akpọ aha ahụ bụ Jehovah ma Ọ bụ Yahweh. Na " +
            "ọtụtụ Akwụkwọ Nsọ, e wepụla aha Chineke ma jiri utu aha bụ Onyenwe Anyị " +
            "ma ọ bụ Chineke dochie ya. Ma mgbe e dere akwụkwọ nsọ, aha ahụ bụ Jehova " +
            "pụtara n’ime ya, ihe dị ka ugboro pụkụ asaa(7,000).";

    public static final String kTestStrIS =
        " a afköst leitarorða þinna leitarorð neikvæð leitarorð auglýsingahópa " +
            "byggja upp aðallista yfir ný leitarorð fyrir auglýsingahópana og skoða " +
            "ítarleg gögn um árangur leitarorða eins og samkeppni auglýsenda og " +
            "leitarmagn er krafist notkun";

    public static final String kTestStrIT =
        " a causa di un intervento di manutenzione del sistema fino alle ore circa " +
            "ora legale costa del pacifico del novembre le campagne esistenti " +
            "continueranno a essere pubblicate come di consueto anche durante questo " +
            "breve periodo di inattività ci scusiamo per";

    public static final String kTestStrIW =
        " או לערוך את העדפות ההפצה אנא עקוב אחרי השלבים הבאים כנס לחשבון האישי שלך " +
            "ב";

    public static final String kTestStrJA =
        " このペ ジでは アカウントに指定された予算の履歴を一覧にしています " +
            "それぞれの項目には 予算額と特定期間のステ タスが表示されます " +
            "現在または今後の予算を設定するには";

    public static final String kTestStrJV =
        "Iki Piece cendhak teks sing bakal digunakake kanggo Testing akurasi " +
            "pendekatan anyar.";

    public static final String kTestStrKA =
        " ა ბირთვიდან მიღებული ელემენტი მენდელეევის პერიოდულ სიტემაში " +
            "გადაინაცვლებს ორი უჯრით";

    public static final String kTestStrKK =
        " а билердің өзіне рұқсат берілмеген егер халық талап етсе ғана хан " +
            "келісім берген өздеріңіз білесіздер қр қыл мыс тық кодексінде жазаның";

    public static final String kTestStrKM =
        "នេះគឺជាបំណែកខ្លីនៃអត្ថបទដែលនឹងត្រូវបានប្រើសម្រាប់ការធ្វើតេស្តភាពត្រឹមត្រូវ" +
            "នៃវិធីសាស្រ្តថ្មីនេះ។";

    public static final String kTestStrKN =
        " ಂಠಯ್ಯನವರು ತುಮಕೂರು ಜಿಲ್ಲೆಯ ಚಿಕ್ಕನಾಯಕನಹಳ್ಳಿ ತಾಲ್ಲೂಕಿನ ತೀರ್ಥಪುರ ವೆಂಬ ಸಾಧಾರಣ" +
            " ಹಳ್ಳಿಯ ಶ್ಯಾನುಭೋಗರ";

    public static final String kTestStrKO =
        " 개별적으로 리포트 액세스 권한을 부여할 수 있습니다 액세스 권한 " +
            "부여사용자에게 프로필 리포트에 액세스할 수 있는 권한을 부여하시려면 가용 " +
            "프로필 상자에서 프로필 이름을 선택한 다음";

    public static final String kTestStrLA =
        " a deo qui enim nocendi causa mentiri solet si iam consulendi causa " +
            "mentiatur multum profecit sed aliud est quod per se ipsum laudabile " +
            "proponitur aliud quod in deterioris comparatione praeponitur aliter enim " +
            "gratulamur cum sanus est homo aliter cum melius";

    public static final String kTestStrLO =
        " ກຫາທົ່ວທັງເວັບ ແລະໃນເວັບໄຮ້ສາຍ ທຳອິດໃຫ້ທຳການຊອກຫາກ່ອນ ຈາກນັ້ນ" +
            " ໃຫ້ກົດປຸ່ມເມນູ ໃນໜ້າຜົນໄດ້";

    public static final String kTestStrLT =
        " a išsijungia mano idėja dėl geriausio laiko po pastarųjų savo santykių " +
            "pasimokiau penki dalykai be kurių negaliu gyventi mano miegamajame tu " +
            "surasi ideali pora išsilavinimas aukštoji mokykla koledžas universitetas " +
            "pagrindinis laipsnis metai";

    public static final String kTestStrLV =
        " a gadskārtējā izpārdošana slēpošana jāņi atlaide izmaiņas trafikā kas " +
            "saistītas ar sezonas izpārdošanu speciālajām atlaidēm u c ir parastas un " +
            "atslēgvārdi kas ir populāri noteiktos laika posmos šajā laikā saņems " +
            "lielāku klikšķu";

    public static final String kTestStrMG =
        " amporisihin i ianao mba hijery ny dika teksta ranofotsiny an ity " +
            "lahatsoratra ity tsy ilaina ny opérateur efa karohina daholo ny teny " +
            "rehetra nosoratanao ampiasao anaovana dokambarotra i google telugu datin " +
            "ny takelaka fikarohana sary renitakelak i";

    public static final String kTestStrMI =
        " haere ki te kainga o o haere ki te kainga o o haere ki te kainga o te " +
            "rapunga ahua o haere ki te kainga o ka tangohia he ki to rapunga kaore au " +
            "mohio te tikanga whakatiki o te ra he whakaharuru te pai rapunga a te " +
            "rapunga ahua a e kainga o nga awhina o te";

    public static final String kTestStrMK =
        " гласовите коалицијата на вмро дпмне како партија со најмногу освоени " +
            "гласови ќе добие евра а на сметката на коализијата за македонија";

    public static final String kTestStrML =
        " ം അങ്ങനെ ഞങ്ങള് അവരുടെ മുമ്പില് നിന്നു ഔടും ഉടനെ നിങ്ങള് പതിയിരിപ്പില് " +
            "നിന്നു എഴുന്നേറ്റു";

    public static final String kTestStrMN =
        " а боловсронгуй болгох орон нутгийн ажил үйлсийг уялдуулж зохицуулах " +
            "дүрэм журам боловсруулах орон нутгийн өмч хөрөнгө санхүүгийн";

    public static final String kTestStrMR =
        "हैदराबाद  उच्चार ऐका (सहाय्य·माहिती)तेलुगू: హైదరాబాదు , उर्दू:" +
            " حیدر آباد हे भारतातील आंध्र प्रदेश राज्याच्या राजधानीचे शहर" +
            " आहे. हैदराबादची लोकसंख्या ७७ लाख ४० हजार ३३४ आहे. मोत्यांचे शहर" +
            " अशी एकेकाळी ओळख असलेल्या या शहराला ऐतिहासिक, सांस्कृतिक आणि " +
            "स्थापत्यशास्त्रीय वारसा लाभला आहे. १९९० नंतर शिक्षण आणि माहिती तंत्रज्ञान" +
            " त्याचप्रमाणे औषधनिर्मिती आणि जैवतंत्रज्ञान क्षेत्रातील उद्योगधंद्यांची" +
            " वाढ शहरात झाली. दक्षिण मध्य भारतातील पर्यटन आणि तेलुगू चित्रपटनिर्मितीचे" +
            " हैदराबाद हे केंद्र आहे";

    public static final String kTestStrMS =
        "pengampunan beramai-ramai supaya mereka pulang ke rumah masing-masing. " +
            "Orang-orang besarnya enggan mengiktiraf sultan yang dilantik oleh Belanda " +
            "sebagai Yang DiPertuan Selangor. Orang ramai pula tidak mahu menjalankan " +
            "perniagaan bijih timah dengan Belanda, selagi raja yang berhak tidak " +
            "ditabalkan. Perdagang yang lain dibekukan terus kerana untuk membalas " +
            "jasa beliau yang membantu Belanda menentang Riau, Johor dan Selangor. Di " +
            "antara tiga orang Sultan juga dipandang oleh rakyat sebagai seorang " +
            "sultan yang paling gigih. 1 | 2 SULTAN Sebagai ganti Sultan Ibrahim " +
            "ditabalkan Raja Muhammad iaitu Raja Muda. Walaupun baginda bukan anak " +
            "isteri pertama bergelar Sultan Muhammad bersemayam di Kuala Selangor " +
            "juga. Pentadbiran baginda yang lemah itu menyebabkan Kuala Selangor " +
            "menjadi sarang ioleh Cina di Lukut tidak diambil tindakan, sedangkan " +
            "baginda sendiri banyak berhutang kepada 1";

    public static final String kTestStrMT =
        " ata ikteb messaġġ lil indirizzi differenti billi tagħżilhom u tagħfas il " +
            "buttuna ikteb żid numri tfittxijja tal kotba mur print home kotba minn " +
            "pagni ghal pagna minn ghall ktieb ta aċċessa stieden habib iehor grazzi " +
            "it tim tal gruppi google";

    public static final String kTestStrMY =
        " တက္ကသုိလ္ မ္ဟ ပ္ရန္ လာ္ရပီးေနာက္ န္ဟစ္ အရ္ဝယ္ ဦးသန္ ့သည္ ပန္" +
            " းတနော္ အမ္ယုိးသား ေက္ယာင္ း";

    public static final String kTestStrNE =
        "अरू ठाऊँबाटपनि खुलेको छ यो खाता अर अरू ठाऊँबाटपनि खुलेको छ यो खाता अर ू";

    public static final String kTestStrNL =
        " a als volgt te werk om een configuratiebestand te maken sitemap gen py " +
            "ebruik filters om de s op te geven die moeten worden toegevoegd of " +
            "uitgesloten op basis van de opmaaktaal elke sitemap mag alleen de s " +
            "bevatten voor een bepaalde opmaaktaal dit";

    public static final String kTestStrNO =
        " a er obligatorisk tidsforskyvning plassering av katalogsøk " +
            "planinformasjon loggfilbane gruppenavn kontoinformasjon passord domene " +
            "gruppeinformasjon alle kampanjesporing alternativ bruker grupper " +
            "oppgaveplanlegger oppgavehistorikk kontosammendrag antall";

    public static final String kTestStrNY =
        "Boma ndi gawo la dziko lomwe linapangidwa ndi cholinga chothandiza " +
            "ntchito yolamulira. Kuŵalako kulikuunikabe mandita, Edipo nyima " +
            "unalephera kugonjetsa kuŵalako.";

    public static final String kTestStrPA =
        " ਂ ਦਿਨਾਂ ਵਿਚ ਭਾਈ ਸਾਹਿਬ ਦੀ ਬੁੱਚੜ ਗੋਬਿੰਦ ਰਾਮ ਨਾਲ ਅੜਫਸ ਚੱਲ ਰਹੀ ਸੀ ਗੋਬਿੰਦ" +
            " ਰਾਮ ਨੇ ਭਾਈ ਸਾਹਿਬ ਦੀਆਂ ਭੈਣਾ";

    public static final String kTestStrPL =
        " a australii będzie widział inne reklamy niż użytkownik z kanady " +
            "kierowanie geograficzne sprawia że reklamy są lepiej dopasowane do " +
            "użytkownika twojej strony oznacza to także że możesz nie zobaczyć " +
            "wszystkich reklam które są wyświetlane na";

    public static final String kTestStrPT =
        " a abit prevê que a entrada desses produtos estrangeiros no mercado " +
            "têxtil e vestuário do brasil possa reduzir os preços em cerca de a partir " +
            "de má notícia para os empresários que terão que lutar para garantir suas " +
            "margens de lucro mas boa notícia";

    public static final String kTestStrRO =
        " a anunţurilor reţineţi nu plătiţi pentru clicuri sau impresii ci numai " +
            "atunci când pe site ul dvs survine o acţiune dorită site urile negative " +
            "nu pot avea uri de destinaţie daţi instrucţiuni societăţii dvs bancare " +
            "sau constructoare să";

    public static final String kTestStrRU =
        " а неправильный формат идентификатора дн назад";

    public static final String kTestStrSI =
        " අනුරාධ මිහිඳුකුල නමින් සකුරා ට ලිපියක් තැපෑලෙන් එවා තිබුණා කි " +
            "් රස්ටි ෂෙල්ටන් ප ් රනාන්දු ද";

    public static final String kTestStrSK =
        " a aktivovať reklamnú kampaň ak chcete kampaň pred spustením ešte " +
            "prispôsobiť uložte ju ako šablónu a pokračujte v úprave vyberte si jednu " +
            "z možností nižšie a kliknite na tlačidlo uložiť kampaň nastavenia kampane " +
            "môžete ľubovoľne";

    public static final String kTestStrSL =
        " adsense stanje prijave za google adsense google adsense račun je bil " +
            "začasno zamrznjen pozdravljeni hvala za vaše zanimanje v google adsense " +
            "po pregledu vaše prijavnice so naši strokovnjaki ugotovili da spletna " +
            "stran ki je trenutno povezana z vašim";

    public static final String kTestStrSO =
        " a oo maanta bogga koobaad ugu qoran yahey beesha caalamka laakiin si " +
            "kata oo beesha caalamku ula guntato soomaaliya waxa aan shaki ku jirin in " +
            "aakhirataanka dadka soomaalida oo kaliya ay yihiin ku soomaaliya ka saari " +
            "kara dhibka ay ku jirto";

    public static final String kTestStrSQ =
        " a do të kërkoni nga beogradi që të njohë pavarësinë e kosovës zoti thaçi " +
            "prishtina është gati ta njoh pavarësinë e serbisë ndërsa natyrisht se do " +
            "të kërkohet një gjë e tillë që edhe beogradi ta njoh shtetin e pavarur " +
            "dhe sovran të";

    public static final String kTestStrSR =
        "балчак балчак на мапи србије уреди демографија у насељу балчак живи " +
            "пунолетна становника а просечна старост становништва износи година";

    public static final String kTestStrST =
        " bang ba nang le thahasello matshwao a sehlooho thuto e thehilweng hodima " +
            "diphetho ke tsela ya ho ruta le ho ithuta e totobatsang hantle seo " +
            "baithuti ba lokelang ho se fihlella ntlhatheo eo e sebetsang ka yona ke " +
            "ya hore titjhere o hlakisa pele seo";

    public static final String kTestStrSU =
        "Nu ngatur kahirupan warga, keur kapentingan pamarentahan diatur ku RT, RW " +
            "jeung Kepala Dusun, sedengkeun urusan adat dipupuhuan ku Kuncen jeung " +
            "kepala adat. Sanajan Kampung Kuta teu pati anggang jeung lembur sejenna " +
            "nu aya di wewengkon Desa Pasir Angin, tapi boh wangunan imah atawa " +
            "tradisi kahirupan masarakatna nenggang ti nu lian.";

    public static final String kTestStrSV =
        " a bort objekt från google desktop post äldst meny öretag dress etaljer " +
            "alternativ för vad är inne yaste google skrivbord plugin program för " +
            "nyheter google visa nyheter som är anpassade efter de artiklar som du " +
            "läser om du till exempel läser";

    public static final String kTestStrSW =
        " a ujumbe mpya jumla unda tafuta na angalia vikundi vya kujadiliana na " +
            "kushiriki mawazo iliyopangwa kwa tarehe watumiaji wapya futa orodha hizi " +
            "lugha hoja vishikanisho vilivyo dhaminiwa ujumbe sanaa na tamasha toka " +
            "udhibitisho wa neno kwa haraka fikia";

    public static final String kTestStrTA =
        " அங்கு ராஜேந்திர சோழனால் கட்டப்பட்ட பிரம்மாண்டமான சிவன் கோவில் ஒன்றும்" +
            " உள்ளது தொகு";

    public static final String kTestStrTE =
        " ఁ దనర జయించిన తత్వ మరసి చూడఁ దాన యగును రాజయోగి యిట్లు తేజరిల్లుచు నుండు " +
            "విశ్వదాభిరామ వినర వేమ";

    public static final String kTestStrTG =
        " адолат ва инсондӯстиро бар фашизм нажодпарастӣ ва адоват тарҷеҳ додааст " +
            "чоп кунед ба дигарон фиристед чоп кунед ба дигарон фиристед";

    public static final String kTestStrTH =
        " กฏในการค้นหา หรือหน้าเนื้อหา หากท่านเลือกลงโฆษณา " +
            "ท่านอาจจะปรับต้องเพิ่มงบประมาณรายวันตา";

    public static final String kTestStrTR =
        " a ayarlarınızı görmeniz ve yönetmeniz içindir eğer kampanyanız için " +
            "günlük bütçenizi gözden geçirebileceğiniz yeri arıyorsanız kampanya " +
            "yönetimi ne gidin kampanyanızı seçin ve kampanya ayarlarını düzenle yi " +
            "tıklayın sunumu";

    public static final String kTestStrUK =
        " а більший бюджет щоб забезпечити собі максимум прибутків від переходів " +
            "відстежуйте свої об яви за датою географічним розташуванням";

    public static final String kTestStrUR =
        " آپ کو کم سے کم ممکنہ رقم چارج کرتا ہے اس کی مثال کے طور پر فرض کریں اگر " +
            "آپ کی زیادہ سے زیادہ قیمت فی کلِک امریکی ڈالر اور کلِک کرنے کی شرح ہو تو";

    public static final String kTestStrUZ =
        " abadiylashtirildi aqsh ayol prezidentga tayyormi markaziy osiyo afg " +
            "onistonga qanday yordam berishi mumkin ukrainada o zbekistonlik " +
            "muhojirlar tazyiqdan shikoyat qilmoqda gruziya va ukraina hozircha natoga " +
            "qabul qilinmaydi afg oniston o zbekistonni g";

    public static final String kTestStrVI =
        " adsense cho nội dung nhà cung cấp dịch vụ di động xác minh tín" +
            " dụng thay đổi nhãn kg các ô xem chi phí cho từ chối các đơn đặt" +
            " hàng dạng cấp dữ liệu ác minh trang web của bạn để xem";

    public static final String kTestStrYI =
        "אן פאנטאזיע ער איז באקאנט צים מערסטן פאר זיינע באַלאַדעס ער האָט געוווינט " +
            "אין ווארשע יעס פאריס ליווערפול און לאנדאן סוף כל סוף איז ער";

    public static final String kTestStrYO =
        " abinibi han ikawe alantakun le ni opolopo ede abinibi ti a to lesese bi " +
            "eniyan to fe lo se fe lati se atunse jowo mo pe awon oju iwe itakunagbaye " +
            "miran ti ako ni oniruru ede abinibi le faragba nipa atunse ninu se iwadi " +
            "blogs ni ori itakun agbaye ti e ba";

    public static final String kTestStrZH =
        "产品的简报和公告 提交该申请后无法进行更改 请确认您的选择是正确的 " +
            "对于要提交的图书 我确认 我是版权所有者或已得到版权所有者的授权 " +
            "要更改您的国家 地区 请在此表的最上端更改您的";

    public static final String kTestStrZU =
        " ana engu uma inkinga iqhubeka siza ubike kwi isexwayiso ngenxa yephutha " +
            "lomlekeleli sikwazi ukubuyisela emuva kuphela imiphumela engaqediwe " +
            "ukuthola imiphumela eqediwe zama ukulayisha kabusha leli khasi emizuzwini " +
            "engu uma inkinga iqhubeka siza uthumele";

    public static final String[][] goldLangText = new String [][]{
        {"af",  kTestStrAF},
        {"ar",  kTestStrAR},
        {"az",  kTestStrAZ},
        {"be",  kTestStrBE},
        {"bg",  kTestStrBG},
        {"bn",  kTestStrBN},
        {"bs",  kTestStrBS},
        {"ca",  kTestStrCA},
        {"ceb", kTestStrCEB},
        {"cs",  kTestStrCS},
        {"cy",  kTestStrCY},
        {"da",  kTestStrDA},
        {"de",  kTestStrDE},
        {"el",  kTestStrEL},
        {"en",  kTestStrEN},
        {"eo",  kTestStrEO},
        {"es",  kTestStrES},
        {"et",  kTestStrET},
        {"eu",  kTestStrEU},
        {"fa",  kTestStrFA},
        {"fi",  kTestStrFI},
        {"fil", kTestStrFIL},
        {"fr",  kTestStrFR},
        {"ga",  kTestStrGA},
        {"gl",  kTestStrGL},
        {"gu",  kTestStrGU},
        {"ha",  kTestStrHA},
        {"hi",  kTestStrHI},
        {"hmn", kTestStrHMN},
        {"hr",  kTestStrHR},
        {"ht",  kTestStrHT},
        {"hu",  kTestStrHU},
        {"hy",  kTestStrHY},
        {"id",  kTestStrID},
        {"ig",  kTestStrIG},
        {"is",  kTestStrIS},
        {"it",  kTestStrIT},
        {"iw",  kTestStrIW},
        {"ja",  kTestStrJA},
        {"jv",  kTestStrJV},
        {"ka",  kTestStrKA},
        {"kk",  kTestStrKK},
        {"km",  kTestStrKM},
        {"kn",  kTestStrKN},
        {"ko",  kTestStrKO},
        {"la",  kTestStrLA},
        {"lo",  kTestStrLO},
        {"lt",  kTestStrLT},
        {"lv",  kTestStrLV},
        {"mg",  kTestStrMG},
        {"mi",  kTestStrMI},
        {"mk",  kTestStrMK},
        {"ml",  kTestStrML},
        {"mn",  kTestStrMN},
        {"mr",  kTestStrMR},
        {"ms",  kTestStrMS},
        {"mt",  kTestStrMT},
        {"my",  kTestStrMY},
        {"ne",  kTestStrNE},
        {"nl",  kTestStrNL},
        {"no",  kTestStrNO},
        {"ny",  kTestStrNY},
        {"pa",  kTestStrPA},
        {"pl",  kTestStrPL},
        {"pt",  kTestStrPT},
        {"ro",  kTestStrRO},
        {"ru",  kTestStrRU},
        {"si",  kTestStrSI},
        {"sk",  kTestStrSK},
        {"sl",  kTestStrSL},
        {"so",  kTestStrSO},
        {"sq",  kTestStrSQ},
        {"sr",  kTestStrSR},
        {"st",  kTestStrST},
        {"su",  kTestStrSU},
        {"sv",  kTestStrSV},
        {"sw",  kTestStrSW},
        {"ta",  kTestStrTA},
        {"te",  kTestStrTE},
        {"tg",  kTestStrTG},
        {"th",  kTestStrTH},
        {"tr",  kTestStrTR},
        {"uk",  kTestStrUK},
        {"ur",  kTestStrUR},
        {"uz",  kTestStrUZ},
        {"vi",  kTestStrVI},
        {"yi",  kTestStrYI},
        {"yo",  kTestStrYO},
        {"zh",  kTestStrZH},
        {"zu",  kTestStrZU}};

    public static final String[][] goldLangResults = new String[][] {
        {"af", "af", "1"},
        {"ar", "ar", "0.999949"},
        {"az", "az", "1"},
        {"be", "be", "0.999994"},
        {"bg", "bg", "0.999996"},
        {"bn", "bn", "0.999985"},
        {"bs", "hr", "0.553621"},
        {"ca", "ca", "1"},
        {"ceb", "ceb", "0.999312"},
        {"cs", "cs", "0.999912"},
        {"cy", "cy", "1"},
        {"da", "da", "0.999977"},
        {"de", "de", "0.998885"},
        {"el", "el", "0.999956"},
        {"en", "en", "0.993561"},
        {"eo", "eo", "0.997801"},
        {"es", "es", "0.999996"},
        {"et", "et", "0.999996"},
        {"eu", "eu", "1"},
        {"fa", "fa", "0.999878"},
        {"fi", "fi", "1"},
        {"fil", "fil", "1"},
        {"fr", "fr", "0.999987"},
        {"ga", "ga", "0.999996"},
        {"gl", "gl", "0.998022"},
        {"gu", "gu", "0.999962"},
        {"ha", "ha", "1"},
        {"hi", "hi", "1"},
        {"hmn", "hmn", "1"},
        {"hr", "hr", "0.522486"},
        {"ht", "ht", "1"},
        {"hu", "hu", "1"},
        {"hy", "hy", "1"},
        {"id", "ms", "0.621919"},
        {"ig", "ig", "1"},
        {"is", "is", "0.999937"},
        {"it", "it", "0.999996"},
        {"iw", "iw", "0.999413"},
        {"ja", "ja", "1"},
        {"jv", "jv", "0.999968"},
        {"ka", "ka", "0.999977"},
        {"kk", "kk", "0.999998"},
        {"km", "km", "0.999996"},
        {"kn", "kn", "0.999977"},
        {"ko", "ko", "0.999989"},
        {"la", "la", "0.999989"},
        {"lo", "lo", "0.999958"},
        {"lt", "lt", "0.999998"},
        {"lv", "lv", "0.999998"},
        {"mg", "mg", "0.999996"},
        {"mi", "mi", "1"},
        {"mk", "mk", "1"},
        {"ml", "ml", "0.999973"},
        {"mn", "mn", "1"},
        {"mr", "mr", "1"},
        {"ms", "ms", "0.849837"},
        {"mt", "mt", "0.999949"},
        {"my", "my", "0.999884"},
        {"ne", "ne", "0.999344"},
        {"nl", "nl", "0.999985"},
        {"no", "no", "0.999922"},
        {"ny", "ny", "0.999985"},
        {"pa", "pa", "0.999926"},
        {"pl", "pl", "1"},
        {"pt", "pt", "0.999992"},
        {"ro", "ro", "1"},
        {"ru", "ru", "0.997257"},
        {"si", "si", "0.999956"},
        {"sk", "sk", "0.999996"},
        {"sl", "sl", "0.974819"},
        {"so", "so", "1"},
        {"sq", "sq", "1"},
        {"sr", "sr", "0.985774"},
        {"st", "st", "0.999998"},
        {"su", "su", "0.992152"},
        {"sv", "sv", "1"},
        {"sw", "sw", "0.999998"},
        {"ta", "ta", "0.999943"},
        {"te", "te", "0.999962"},
        {"tg", "tg", "0.999949"},
        {"th", "th", "0.999981"},
        {"tr", "tr", "1"},
        {"uk", "uk", "1"},
        {"ur", "ur", "0.999998"},
        {"uz", "uz", "0.999367"},
        {"vi", "vi", "0.999996"},
        {"yi", "yi", "0.999954"},
        {"yo", "yo", "0.999836"},
        {"zh", "zh", "0.999977"},
        {"zu", "zu", "0.999989"}};
}
