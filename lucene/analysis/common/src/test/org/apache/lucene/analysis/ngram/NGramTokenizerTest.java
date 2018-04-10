/*
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
package org.apache.lucene.analysis.ngram;



import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

/**
 * Tests {@link NGramTokenizer} for correctness.
 */
public class NGramTokenizerTest extends BaseTokenStreamTestCase {
  private StringReader input;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    input = new StringReader("abcde");
  }
  
  public void testInvalidInput() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {
      new NGramTokenizer(2, 1);
    });
  }
  
  public void testInvalidInput2() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {
      @SuppressWarnings("resource")
      NGramTokenizer tokenizer = new NGramTokenizer(0, 1);
      tokenizer.setReader(input);
    });
  }
  
  public void testUnigrams() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(1, 1);
    tokenizer.setReader(new StringReader("abcdefghijklmnopq"));
    assertTokenStreamContents(tokenizer,
        new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k",
            "l", "m", "n", "o", "p", "q"},
        new int[] {0, 1, 2, 3, 4, 5,6,7,8,9,10,11,12,13,14,15,16},
        new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12,13,14,15,16,17}, 17 );
  }
  
  public void testBigrams() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(2, 2);
    tokenizer.setReader(input);
    assertTokenStreamContents(tokenizer, new String[]{"ab","bc","cd","de"}, new int[]{0,1,2,3}, new int[]{2,3,4,5}, 5 /* abcde */);
  }
  
  public void testNgrams() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(1, 3);
    tokenizer.setReader(input);
    assertTokenStreamContents(tokenizer,
        new String[]{"a","ab", "abc", "b", "bc", "bcd", "c", "cd", "cde", "d", "de", "e"},
        new int[]{0,0,0,1,1,1,2,2,2,3,3,4},
        new int[]{1,2,3,2,3,4,3,4,5,4,5,5},
        null,
        null,
        null,
        5 /* abcde */,
        false
        );
  }
  
  public void testOversizedNgrams() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(6, 7);
    tokenizer.setReader(input);
    assertTokenStreamContents(tokenizer, new String[0], new int[0], new int[0], 5 /* abcde */);
  }
  
  public void testOversizedNgramsKeepShortTerm() throws Exception {
    {
      NGramTokenizer tokenizer = new NGramTokenizer(6, 7, true, false);
      tokenizer.setReader(input);
      assertTokenStreamContents(tokenizer, new String[] {"abcde"}, new int[] {0}, new int[] {5} , 5 /* abcde */);  
    }
    
    {
      NGramTokenizer tokenizer = new NGramTokenizer(5,6, true, false) {
        @Override
        protected boolean isTokenChar(int chr) {
          return chr != ' ';
        }
      };
      tokenizer.setReader(new StringReader(" a bcd  efghij  x  "));
      assertTokenStreamContents(
          tokenizer,
          new String[]{"a", "bcd", "efghi", "efghij", "fghij", "x"},
          new int[]   {  1,     3,      8,         8,       9,  16},
          new int[]   {  2,     6,      13,       14,      14,  17},
          19);
    }
  }
  
  public void testKeepShortTermKeepLongTerm() throws Exception {
    final String inputString = "a bcd efghi jk";

    { // default behaviour
      NGramTokenizer tokenizer = new NGramTokenizer(2,3, false, false) {
        @Override
        protected boolean isTokenChar(int chr) {
          return chr != ' ';
        }
      };
      
      tokenizer.setReader(new StringReader(inputString));
      assertTokenStreamContents(tokenizer,
          new String[] { "bc", "bcd",  "cd", "ef", "efg", "fg", "fgh", "gh", "ghi", "hi", "jk" },
          new int[]    {    2,     2,     3,    6,     6,    7,     7,    8,     8,    9,   12 },
          new int[]    {    4,     5,     5,    8,     9,    9,    10,   10,    11,   11,   14 },
          new int[]    {    1,     1,     1,    1,     1,    1,     1,    1,     1,    1,    1 });
    }

    { // keepShortTerm && keepLongTerm
      NGramTokenizer tokenizer = new NGramTokenizer(2,3, true, true) {
        @Override
        protected boolean isTokenChar(int chr) {
          return chr != ' ';
        }
      };
      
      tokenizer.setReader(new StringReader(inputString));
      assertTokenStreamContents(tokenizer,
          new String[] { "a", "bc", "bcd",  "cd", "ef", "efg", "efghi", "fg", "fgh", "gh", "ghi", "hi", "jk" },
          new int[]    {   0,    2,     2,     3,    6,     6,       6,    7,     7,    8,     8,    9,   12 },
          new int[]    {   1,    4,     5,     5,    8,     9,      11,    9,    10,   10,    11,   11,   14 },
          new int[]    {   1,    1,     1,     1,    1,     1,       1,    1,     1,    1,     1,    1,    1 });
    }
    
    { // keepShortTerm && !keepLongTerm
      NGramTokenizer tokenizer = new NGramTokenizer(2,3, true, false) {
        @Override
        protected boolean isTokenChar(int chr) {
          return chr != ' ';
        }
      };
      
      tokenizer.setReader(new StringReader(inputString));
      assertTokenStreamContents(tokenizer,
          new String[] { "a", "bc", "bcd",  "cd", "ef",  "efg", "fg", "fgh", "gh", "ghi", "hi", "jk" },
          new int[]    {   0,    2,     2,     3,    6,      6,    7,     7,    8,     8,    9,   12 },
          new int[]    {   1,    4,     5,     5,    8,      9,    9,    10,   10,    11,   11,   14 },
          new int[]    {   1,    1,     1,     1,    1,      1,    1,     1,    1,     1,    1,    1 });
    }
    
    { // !keepShortTerm && keepLongTerm
      NGramTokenizer tokenizer = new NGramTokenizer(2,3, false, true) {
        @Override
        protected boolean isTokenChar(int chr) {
          return chr != ' ';
        }
      };
      
      tokenizer.setReader(new StringReader(inputString));
      assertTokenStreamContents(tokenizer,
          new String[] { "bc", "bcd",  "cd", "ef", "efg", "efghi", "fg", "fgh", "gh", "ghi", "hi", "jk" },
          new int[]    {    2,     2,     3,    6,     6,       6,    7,     7,    8,     8,    9,   12 },
          new int[]    {    4,     5,     5,    8,     9,      11,    9,    10,   10,    11,   11,   14 },
          new int[]    {    1,     1,     1,    1,     1,       1,    1,     1,    1,     1,    1,    1 });
    }
  }
  
  public void testKeepLongTermHugeTerms() throws IOException {
    NGramTokenizer tokenizer = new NGramTokenizer(1,2, false, true) {
      @Override
      protected boolean isTokenChar(int chr) {
        return chr != ' ';
      }
    };
    
    List<String> inputStrings = new ArrayList<>();
    
    for (int i = 0; i < 1;) {
      String currentString = TestUtil.randomUnicodeString(random(), 128);
      if (currentString.length() > 4) {
        inputStrings.add(StringUtils.replace(currentString, " ", "-"));
        i++;
      }
    }
    StringBuilder sb = new StringBuilder();
    for (String inputString : inputStrings) {
      sb.append(inputString);
      for (int i = 0; i < random().nextInt(4) + 1; i++)
      sb.append(" ");
    }
    
    String input = sb.toString();
    tokenizer.setReader(new StringReader(input));
    tokenizer.reset();
  
    List<String> resultStrings = new ArrayList<>();
    while(tokenizer.incrementToken()) {
      CharTermAttribute termAttr = tokenizer.getAttribute(CharTermAttribute.class);
      if (termAttr.length() > 4) {
        resultStrings.add(termAttr.toString());
      }
    }
    
    assertEquals(inputStrings.size(), resultStrings.size());
    for (int i = 0; i < inputStrings.size(); i++) {
      assertEquals(inputStrings.get(i), resultStrings.get(i));
    }
    
    tokenizer.close();
  }
  
  public void testReset() throws Exception {
    NGramTokenizer tokenizer = new NGramTokenizer(1, 1);
    tokenizer.setReader(input);
    assertTokenStreamContents(tokenizer, new String[]{"a","b","c","d","e"}, new int[]{0,1,2,3,4}, new int[]{1,2,3,4,5}, 5 /* abcde */);
    tokenizer.setReader(new StringReader("abcde"));
    assertTokenStreamContents(tokenizer, new String[]{"a","b","c","d","e"}, new int[]{0,1,2,3,4}, new int[]{1,2,3,4,5}, 5 /* abcde */);
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    int numIters = TEST_NIGHTLY ? 16 : 4;
    for (int i = 0; i < numIters; i++) {
      final int min = TestUtil.nextInt(random(), 2, 10);
      final int max = TestUtil.nextInt(random(), min, 20);
      final boolean keepShortTerm = i % 2 == 0;
      final boolean keepLongTerm = i % 4 == 0;
      
      Analyzer a = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new NGramTokenizer(min, max, keepShortTerm, keepLongTerm);
          return new TokenStreamComponents(tokenizer, tokenizer);
        }    
      };
      checkRandomData(random(), a, 200*RANDOM_MULTIPLIER, 20);
      checkRandomData(random(), a, 10*RANDOM_MULTIPLIER, 1027);
      a.close();
    }
  }

  private static void testNGrams(int minGram, int maxGram, int length, final String nonTokenChars) throws IOException {
    final String s = RandomStrings.randomAsciiLettersOfLength(random(), length);
    testNGrams(minGram, maxGram, s, nonTokenChars);
  }

  private static void testNGrams(int minGram, int maxGram, String s, String nonTokenChars) throws IOException {
    testNGrams(minGram, maxGram, s, nonTokenChars, false);
  }

  static int[] toCodePoints(CharSequence s) {
    final int[] codePoints = new int[Character.codePointCount(s, 0, s.length())];
    for (int i = 0, j = 0; i < s.length(); ++j) {
      codePoints[j] = Character.codePointAt(s, i);
      i += Character.charCount(codePoints[j]);
    }
    return codePoints;
  }

  static boolean isTokenChar(String nonTokenChars, int codePoint) {
    for (int i = 0; i < nonTokenChars.length(); ) {
      final int cp = nonTokenChars.codePointAt(i);
      if (cp == codePoint) {
        return false;
      }
      i += Character.charCount(cp);
    }
    return true;
  }

  static void testNGrams(int minGram, int maxGram, String s, final String nonTokenChars, boolean edgesOnly) throws IOException {
    // convert the string to code points
    final int[] codePoints = toCodePoints(s);
    final int[] offsets = new int[codePoints.length + 1];
    for (int i = 0; i < codePoints.length; ++i) {
      offsets[i+1] = offsets[i] + Character.charCount(codePoints[i]);
    }
    final Tokenizer grams = new NGramTokenizer(minGram, maxGram, edgesOnly) {
      @Override
      protected boolean isTokenChar(int chr) {
        return nonTokenChars.indexOf(chr) < 0;
      }
    };
    grams.setReader(new StringReader(s));
    final CharTermAttribute termAtt = grams.addAttribute(CharTermAttribute.class);
    final PositionIncrementAttribute posIncAtt = grams.addAttribute(PositionIncrementAttribute.class);
    final PositionLengthAttribute posLenAtt = grams.addAttribute(PositionLengthAttribute.class);
    final OffsetAttribute offsetAtt = grams.addAttribute(OffsetAttribute.class);
    grams.reset();
    for (int start = 0; start < codePoints.length; ++start) {
      nextGram:
      for (int end = start + minGram; end <= start + maxGram && end <= codePoints.length; ++end) {
        if (edgesOnly && start > 0 && isTokenChar(nonTokenChars, codePoints[start - 1])) {
          // not on an edge
          continue nextGram;
        }
        for (int j = start; j < end; ++j) {
          if (!isTokenChar(nonTokenChars, codePoints[j])) {
            continue nextGram;
          }
        }
        assertTrue(grams.incrementToken());
        assertArrayEquals(Arrays.copyOfRange(codePoints, start, end), toCodePoints(termAtt));
        assertEquals(1, posIncAtt.getPositionIncrement());
        assertEquals(1, posLenAtt.getPositionLength());
        assertEquals(offsets[start], offsetAtt.startOffset());
        assertEquals(offsets[end], offsetAtt.endOffset());
      }
    }
    assertFalse(grams.incrementToken());
    grams.end();
    assertEquals(s.length(), offsetAtt.startOffset());
    assertEquals(s.length(), offsetAtt.endOffset());
    grams.close();
  }

  public void testLargeInput() throws IOException {
    // test sliding
    final int minGram = TestUtil.nextInt(random(), 1, 100);
    final int maxGram = TestUtil.nextInt(random(), minGram, 100);
    testNGrams(minGram, maxGram, TestUtil.nextInt(random(), 3 * 1024, 4 * 1024), "");
  }

  public void testLargeMaxGram() throws IOException {
    // test sliding with maxGram > 1024
    final int minGram = TestUtil.nextInt(random(), 1290, 1300);
    final int maxGram = TestUtil.nextInt(random(), minGram, 1300);
    testNGrams(minGram, maxGram, TestUtil.nextInt(random(), 3 * 1024, 4 * 1024), "");
  }

  public void testPreTokenization() throws IOException {
    final int minGram = TestUtil.nextInt(random(), 1, 100);
    final int maxGram = TestUtil.nextInt(random(), minGram, 100);
    testNGrams(minGram, maxGram, TestUtil.nextInt(random(), 0, 4 * 1024), "a");
  }

  public void testHeavyPreTokenization() throws IOException {
    final int minGram = TestUtil.nextInt(random(), 1, 100);
    final int maxGram = TestUtil.nextInt(random(), minGram, 100);
    testNGrams(minGram, maxGram, TestUtil.nextInt(random(), 0, 4 * 1024), "abcdef");
  }

  public void testFewTokenChars() throws IOException {
    final char[] chrs = new char[TestUtil.nextInt(random(), 4000, 5000)];
    Arrays.fill(chrs, ' ');
    for (int i = 0; i < chrs.length; ++i) {
      if (random().nextFloat() < 0.1) {
        chrs[i] = 'a';
      }
    }
    final int minGram = TestUtil.nextInt(random(), 1, 2);
    final int maxGram = TestUtil.nextInt(random(), minGram, 2);
    testNGrams(minGram, maxGram, new String(chrs), " ");
  }

  public void testFullUTF8Range() throws IOException {
    final int minGram = TestUtil.nextInt(random(), 1, 100);
    final int maxGram = TestUtil.nextInt(random(), minGram, 100);
    final String s = TestUtil.randomUnicodeString(random(), 4 * 1024);
    testNGrams(minGram, maxGram, s, "");
    testNGrams(minGram, maxGram, s, "abcdef");
  }
}
