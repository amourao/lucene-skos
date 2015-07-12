package at.ac.univie.mminf.luceneSKOS.analysis.tokenattributes;

/**
 * Copyright 2010 Bernhard Haslhofer 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.Attribute;

/**
 * This class represents SKOS-specific meta-information that is assigned to
 * tokens during the analysis phase.
 * 
 * Note: when tokens are posted to the index as terms, attribute information is
 * lost unless it is encoded in the terms' payload.
 */
public interface SKOSTypeAttribute extends Attribute {
  
  /**
   * An enumeration of supported SKOS concept types
   */
  public static enum SKOSType {
    
    PREF, ALT, HIDDEN, BROADER, NARROWER, BROADERTRANSITIVE, NARROWERTRANSITIVE, RELATED
    ,BROADER1,BROADER2,BROADER3,BROADER4,BROADER5,BROADER6,BROADER7,BROADER8,BROADER9,BROADER10,BROADER11,BROADER12
    ,NARROWER1,NARROWER2,NARROWER3,NARROWER4,NARROWER5,NARROWER6,NARROWER7,NARROWER8,NARROWER9,NARROWER10,NARROWER11,NARROWER12;
    
    /**
     * Returns the SKOSType given the ordinal.
     */
    private static SKOSType fromInteger(int ordinal) {
    	return SKOSType.values()[ordinal];
    }
  }
  
  /**
   * Returns the SKOS type
   * 
   * @return SKOSType
   */
  SKOSType getSkosType();
  
  /**
   * Sets this Token's SKOSType.
   * 
   * @param skosType
   */
  void setSkosType(SKOSType skosType);
}
