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

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.privacy.PrivacyConstraint;
import org.apache.sysds.runtime.privacy.PrivacyConstraint.PrivacyLevel;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

public class FederatedRewriteTest extends AutomatedTestBase {
	private static final String TEST_DIR = "functions/privacy/";
	private final static String TEST_CLASS_DIR = TEST_DIR + FederatedRewriteTest.class.getSimpleName() + "/";
	private final static String TEST_NAME_1 = "RewriteTest1";
	private final static String TEST_NAME_2 = "RewriteTest2";
	private final static String TEST_NAME_3 = "RewriteTest3";
	private int m = 4;
	private final int n = 20;
	private final int k = 1;

	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME_1,
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME_1, new String[]{"c"}));
		addTestConfiguration(TEST_NAME_2,
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME_2, new String[]{"c"}));
		addTestConfiguration(TEST_NAME_3,
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME_3, new String[]{"c"}));
	}

	@Test
	public void singleFederatedSingleConstraint(){
		PrivacyConstraint constraint1 = new PrivacyConstraint(PrivacyLevel.PrivateAggregation);
		generalTest(constraint1, null, false, TEST_NAME_2, null, false);
	}

	@Test
	@Ignore
	/**
	 * Matrix-matrix binary operations with a federated right input are not supported for this case yet.
	 * When it is supported, this test case should still throw an exception.
	 */
	public void doubleFederatedDoubleConstraint(){
		PrivacyConstraint constraint12 = new PrivacyConstraint(PrivacyLevel.PrivateAggregation);
		generalTest(constraint12, constraint12, true, TEST_NAME_1, DMLRuntimeException.class, false);
	}

	@Test
	public void crossOverFederatedFirstOperandConstrained(){
		PrivacyConstraint constraint1 = new PrivacyConstraint(PrivacyLevel.PrivateAggregation);
		m = 1;
		generalTest(constraint1, null, true, TEST_NAME_3, null, true);
	}

	@Test
	public void crossOverFederatedSecondOperandConstrained(){
		PrivacyConstraint constraint2 = new PrivacyConstraint(PrivacyLevel.PrivateAggregation);
		m = 1;
		generalTest(null, constraint2, true, TEST_NAME_3, null, true);
	}



	private void generalTest(PrivacyConstraint privacyConstraint1, PrivacyConstraint privacyConstraint2, boolean doubleFederated,
		String testName, Class expectedException, boolean checkExpected){
		Types.ExecMode platformOld = setExecMode(Types.ExecMode.SINGLE_NODE);
		getAndLoadTestConfiguration(testName);
		String HOME = SCRIPT_DIR + TEST_DIR;

		int halfRows = n/2;
		double[][] X1 = getRandomMatrix(halfRows, m, -1, 1, 1, 1);
		double[][] X2 = getRandomMatrix(halfRows, m, -1, 1, 1, 2);
		MatrixCharacteristics mcX = new MatrixCharacteristics(halfRows,m,n,halfRows*m);
		writeInputMatrixWithMTD("X1", X1, false, mcX, privacyConstraint1);
		writeInputMatrixWithMTD("X2", X2, false, mcX, privacyConstraint1);

		if (doubleFederated){
			double[][] Y1 = getRandomMatrix(halfRows, k, -1, 1, 1, 3);
			double[][] Y2 = getRandomMatrix(halfRows, k, -1, 1, 1, 4);
			MatrixCharacteristics mcY = new MatrixCharacteristics(halfRows,1,n,halfRows);
			writeInputMatrixWithMTD("Y1", Y1, false, mcY, privacyConstraint2);
			writeInputMatrixWithMTD("Y2", Y2, false, mcY, privacyConstraint2);
			if (checkExpected){
				double[][] c = TestUtils.performMatrixMultiplication(TestUtils.performTranspose(Y2), X1);
				writeExpectedMatrix("c", c);
			}
		} else {
			MatrixCharacteristics mcY = new MatrixCharacteristics(n,1,n,n);
			double[][] Y = getRandomMatrix(n, k, -1, 1,1, 3);
			writeInputMatrixWithMTD("Y", Y, false, mcY, null);
		}

		int workerPort1 = getRandomAvailablePort();
		int workerPort2 = getRandomAvailablePort();
		Thread workerThread1 = startLocalFedWorkerThread(workerPort1, FED_WORKER_WAIT_S);
		Thread workerThread2 = startLocalFedWorkerThread(workerPort2);

		TestConfiguration config = availableTestConfigurations.get(testName);
		loadTestConfiguration(config);
		fullDMLScriptName = HOME + testName + ".dml";

		if ( doubleFederated ){
			programArgs = new String[] {
				"-stats",
				"-explain",
				"-nvargs",
				"X1=" + TestUtils.federatedAddress(workerPort1, input("X1")),
				"X2=" + TestUtils.federatedAddress(workerPort2, input("X2")),
				"Y1=" + TestUtils.federatedAddress(workerPort1, input("Y1")),
				"Y2=" + TestUtils.federatedAddress(workerPort2, input("Y2")),
				"n=" + n,
				"m=" + m,
				"c=" + output("c")
			};
		} else {
			programArgs = new String[] {
				"-stats",
				"-explain",
				"-nvargs",
				"X1=" + TestUtils.federatedAddress(workerPort1, input("X1")),
				"X2=" + TestUtils.federatedAddress(workerPort2, input("X2")),
				"Y=" + input("Y"),
				"n=" + n,
				"m=" + m,
				"c=" + output("c")
			};
		}

		runTest(true, expectedException != null, expectedException, -1);
		if ( checkExpected )
			compareResults(1e-9);


		TestUtils.shutdownThreads(workerThread1, workerThread2);
		resetExecMode(platformOld);
	}
}
