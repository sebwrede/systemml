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

package org.apache.sysds.runtime.instructions.fed;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.instructions.cp.TernaryCPInstruction;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.TernaryOperator;

import java.util.Arrays;
import java.util.concurrent.Future;

public class TernaryFEDInstruction extends FEDInstruction{

	private final CPOperand[] inputOperands;
	private final CPOperand outputOperand;

	protected TernaryFEDInstruction(TernaryOperator op, String opcode, String istr, boolean federatedOutput,
		CPOperand[] inputOperands, CPOperand outputOperand){
		super(FEDType.Ternary, op, opcode, istr, federatedOutput);
		this.inputOperands = inputOperands;
		this.outputOperand = outputOperand;
	}

	public static TernaryFEDInstruction parseInstruction(TernaryCPInstruction inst) {
		return parseInstruction(inst.getInstructionString());
	}

	public static TernaryFEDInstruction parseInstruction(String inst){
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(inst);
		String opcode=parts[0];
		CPOperand[] inputOperands = new CPOperand[3];
		for ( int i = 0; i < 3; i++)
			inputOperands[i] = new CPOperand(parts[i+1]);
		CPOperand outOperand = new CPOperand(parts[4]);
		int numThreads = parts.length>5 ? Integer.parseInt(parts[5]) : 1;
		boolean federatedOutput = parts.length > 6 && parts[6].equals("true");
		TernaryOperator op = InstructionUtils.parseTernaryOperator(opcode, numThreads);
		return new TernaryFEDInstruction(op, opcode, inst, federatedOutput, inputOperands, outOperand);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		Data dataObject1 = ec.getVariable(inputOperands[0]);
		Data dataObject2 = ec.getVariable(inputOperands[1]);
		Data dataObject3 = ec.getVariable(inputOperands[2]);

		if ( dataObject1 instanceof MatrixObject &&
			dataObject2 instanceof ScalarObject &&
			dataObject3 instanceof MatrixObject){
			MatrixObject mo1 = (MatrixObject)dataObject1;
			ScalarObject so2 = (ScalarObject)dataObject2;
			MatrixObject mo3 = (MatrixObject)dataObject3;
			if ( mo3.isFederated() ){
				if ( _federatedOutput )
					broadcastInput1Process(mo1, so2, mo3, ec);
				else
					broadcastInput1ProcessGet(mo1, so2, mo3, ec);
			}
			else
				throw new DMLRuntimeException("Federated Ternary only supported when input three is federated. "
					+ "Input 3 is:" + mo3.getFileName());
		} else {
			throw new DMLRuntimeException("Federated Ternary not supported with the "
				+ "following data types: " + dataObject1.getDataType()+ ":"
				+ dataObject2.getDataType() + " " + dataObject3.getDataType());
		}
	}

	/**
	 * Broadcast first and second input to federated mapping of third input and execute instruction.
	 * Output stays federated.
	 * @param mo1 first input as a matrix object
	 * @param so2 second input as a scalar object
	 * @param mo3 third input as a matrix object with federated mapping
	 * @param ec execution context
	 */
	private void broadcastInput1Process(MatrixObject mo1, ScalarObject so2, MatrixObject mo3, ExecutionContext ec){
		FederatedRequest[] executionRequests = generateExecutionRequests(mo1, so2, mo3);
		mo3.getFedMapping().execute(getTID(), executionRequests);
		long instructionOutputID = executionRequests[executionRequests.length-1].getID();
		setOutputFedMapping(mo1, mo3, instructionOutputID, ec);
	}

	/**
	 * Broadcast first and second input to federated mapping of third input and execute instruction.
	 * Output is retrieved and broadcast variables are removed from federated workers.
	 * @param mo1 first input as a matrix object
	 * @param so2 second input as a scalar object
	 * @param mo3 third input as a matrix object with federated mapping
	 * @param ec execution context
	 */
	private void broadcastInput1ProcessGet(MatrixObject mo1, ScalarObject so2, MatrixObject mo3, ExecutionContext ec){
		FederatedRequest[] executionRequests = generateExecutionRequests(mo1, so2, mo3);
		long instructionOutputID = executionRequests[executionRequests.length-1].getID();
		FederatedRequest getRequest = new FederatedRequest(FederatedRequest.RequestType.GET_VAR, instructionOutputID);
		FederatedRequest rmRequest = mo3.getFedMapping().cleanup(getTID(),
			Arrays.stream(executionRequests).mapToLong(request -> request.getID()).toArray());
		Future<FederatedResponse>[] outputResponses = mo3.getFedMapping().execute(
			getTID(), executionRequests, getRequest, rmRequest);
		MatrixBlock ret = FederationUtils.bind(outputResponses, false);
		ec.setMatrixOutput(outputOperand.getName(), ret);
	}

	/**
	 * Generate federated requests related to broadcasting and execution of the instruction.
	 * @param mo1 first input as a matrix object
	 * @param so2 second input as a scalar object
	 * @param mo3 third input as a matrix object with federated mapping
	 * @return array of federated requests related to broadcasting and execution
	 */
	private FederatedRequest[] generateExecutionRequests(MatrixObject mo1, ScalarObject so2, MatrixObject mo3){
		FederatedRequest[] matrixSlicedBroadcast = mo3.getFedMapping().broadcastSliced(mo1, false);
		FederatedRequest scalarBroadcast = mo3.getFedMapping().broadcast(so2);
		FederatedRequest executionRequest = FederationUtils.callInstruction(getInstructionString(),
			outputOperand,
			inputOperands,
			new long[] {matrixSlicedBroadcast[0].getID(), scalarBroadcast.getID(), mo3.getFedMapping().getID()},
			_federatedOutput);
		return collectRequests(matrixSlicedBroadcast, scalarBroadcast, executionRequest);
	}

	/**
	 * Collect federated requests into a single array of federated requests.
	 * The federated requests are added in the same order as the parameters of this method.
	 * @param fedRequests array of federated requests
	 * @param fedRequest1 federated request to occur after array
	 * @param fedRequest2 federated request to occur after fedRequest1
	 * @return federated requests collected in a single array
	 */
	private FederatedRequest[] collectRequests(FederatedRequest[] fedRequests,
		FederatedRequest fedRequest1, FederatedRequest fedRequest2){
		FederatedRequest[] allRequests = new FederatedRequest[fedRequests.length + 2];
		for ( int i = 0; i < fedRequests.length; i++ )
			allRequests[i] = fedRequests[i];
		allRequests[allRequests.length-2] = fedRequest1;
		allRequests[allRequests.length-1] = fedRequest2;
		return allRequests;
	}

	/**
	 * Set data characteristics and fed mapping for output.
	 * @param dcObject federated matrix object from which data characteristics is derived
	 * @param fedMapObject federated matrix object from which federated mapping is derived
	 * @param outputFedMapID ID for the fed mapping of output
	 * @param ec execution context
	 */
	private void setOutputFedMapping(MatrixObject dcObject, MatrixObject fedMapObject, long outputFedMapID, ExecutionContext ec){
		MatrixObject out = ec.getMatrixObject(outputOperand);
		out.getDataCharacteristics().set(dcObject.getNumRows(), dcObject.getNumColumns(), (int)dcObject.getBlocksize());
		out.setFedMapping(fedMapObject.getFedMapping().copyWithNewID(outputFedMapID, dcObject.getNumColumns()));
	}
}
