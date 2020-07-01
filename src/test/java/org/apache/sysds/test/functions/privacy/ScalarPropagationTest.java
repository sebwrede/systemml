/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.test.functions.privacy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;

import org.junit.Test;
import org.apache.sysds.parser.DataExpression;
import org.apache.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.apache.sysds.runtime.privacy.PrivacyConstraint;
import org.apache.sysds.runtime.privacy.PrivacyConstraint.PrivacyLevel;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.apache.wink.json4j.JSONException;

public class ScalarPropagationTest extends AutomatedTestBase 
{
	
	private final static String TEST_NAME = "ScalarPropagationTest";
	private final static String TEST_DIR = "functions/privacy/";
	private final static String TEST_CLASS_DIR = TEST_DIR + ScalarPropagationTest.class.getSimpleName() + "/";

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration("ScalarPropagationTest", new TestConfiguration(TEST_CLASS_DIR, "ScalarPropagationTest", new String[] { "scalar" }));
	}
	
	@Test
	public void testRound() {
		getAndLoadTestConfiguration(TEST_NAME);

		/* This is for running the junit test the new way, i.e., construct the arguments directly */
		String HOME = SCRIPT_DIR + TEST_DIR;
		fullDMLScriptName = HOME + TEST_NAME + ".dml";
		programArgs = new String[]{"-args", input("A"), output("scalar") };

		double scalar = 10.7;
		double[][] A = {{scalar}};
		writeInputMatrixWithMTD("A", A, true, new PrivacyConstraint(PrivacyLevel.Private));
		
		double roundScalar = Math.round(scalar);

		writeExpectedScalar("scalar", roundScalar);
		
		runTest(true, false, null, -1);
		
		HashMap<CellIndex, Double> map = readDMLScalarFromHDFS("scalar");
		double dmlvalue = map.get(new CellIndex(1,1));
		
		if ( dmlvalue != roundScalar ) {
			throw new RuntimeException("Values mismatch: DMLvalue " + dmlvalue + " != ExpectedValue " + roundScalar);
		}

		try{
			String actualPrivacyValue = readDMLMetaDataValue("scalar", "out/", DataExpression.PRIVACY);
			assertEquals(String.valueOf(PrivacyLevel.Private), actualPrivacyValue);
		} catch (JSONException | NullPointerException e){
			fail("Privacy constraint not written to output metadata file:\n" + e);
		}
	}
}
