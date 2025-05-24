package org.example;

import company.evo.jmorphy2.MorphAnalyzer;
import company.evo.jmorphy2.nlp.SimpleTagger;
import morfologik.stemming.Dictionary;
import morfologik.stemming.DictionaryLookup;
import morfologik.stemming.WordData;

import java.net.URL;
import java.util.Enumeration;
import java.util.List;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        try {
//            URL url = Main.class.getClassLoader().getResource("company/evo/jmorphy2/uk/pymorphy2_dicts");
//            assert url != null;
//            MorphAnalyzer analyzer = new MorphAnalyzer.Builder().fileLoader(new JarFileLoader("company/evo/jmorphy2/uk/pymorphy2_dicts")).build();
//            SimpleTagger tagger = new SimpleTagger(analyzer);
//
//            String sentence = "Це речення для аналізу .";
//            System.out.println(tagger.tag(sentence.split(" ")));

//            System.out.println(Main.class.getClassLoader().getResource("ua/net/nlp/"));

//            Dictionary dictionary = Dictionary.read(Main.class.getClassLoader().getResource("org/languagetool/resource/uk/ukrainian.dict"));
//            DictionaryLookup lookup = new DictionaryLookup(dictionary);

//            List<WordData> results = lookup.lookup("будинки");
//            for (WordData word : results) {
//                System.out.println(word);
//            }

            GpuLemmatizer lemmatizer = new GpuLemmatizer();
            String[] lemmatized = lemmatizer.lemmatize(new String[] {
                    "теплому",     // adjective: masc, dat
                    "ящірки",      // noun: gen sg
                    "синього",     // adjective: masc, gen
                    "українська",  // adjective: fem nom
                    "ходив",       // verb: masc past

                    // More noun forms
                    "двері",       // noun: nom pl
                    "чоловіка",    // noun: gen sg
                    "жінці",       // noun: dat sg
                    "вікном",      // noun: ins sg
                    "містах",      // noun: loc pl

                    // Verb forms
                    "читала",      // verb: fem past
                    "пишемо",      // verb: 1pl pres
                    "розмовляєш",  // verb: 2sg pres
                    "поїхав",      // verb: masc past
                    "буду",        // verb: 1sg fut

                    // Adjective/participle/etc.
                    "старіший",    // comparative
                    "найбільший",  // superlative
                    "відомому",    // adjective: masc, loc
                    "знайдену",     // participle/adjective

                    // Random test cases
                    "невідоме",    // neuter adjective
                    "новини",      // noun: pl nom/acc
                    "книжками",    // noun: ins pl
                    "допомагаючи", // gerund
                    "бігатимеш"    // verb: 2sg fut
            });
            for (String lemma : lemmatized) {
                System.out.println(lemma);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}