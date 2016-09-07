package Transaction;

import java.util.Date;

public class RiskIndicesMonitorAPI {
	//API, programming interface
    static void runRiskIndicesMonitor(){
    	//Running from beginning date, which is the first pay date among the whole batch of loans till the designated
		//ending date that concludes this monitoring (i.e. wholly refreshing, instead of delta processing, 
		//delta-ing it would be in next development schedule), this process populates each and every column on the 
		//historical_risk_index table for next stage of reporting and modeling. 
		//Caveat: Follow steps below to run, these steps are continuous, indivisible and sequential, 
		//stopping halfway or executing them in illegal order would cause programme error and database tables in undefined, incorrect state.
				
		//Starting...
		//Step 1, PaymentProcessor.paymentScheduleOutLaying()
		PaymentProcessor.paymentScheduleOutLaying();
		@SuppressWarnings("deprecation")
		//designate the ending date, beginning date is extracted from the portfolio's payment table, eye-balling it at the moment. 
		Date beginningRunDate=new Date("10/18/2015");
		Date endingRunDate=new Date("12/31/2015");
		//step 2, PaymentProcessor.fulfillmentRate()
		PaymentProcessor.fulfillmentRate(beginningRunDate,endingRunDate);
		//step 3, Loan.readInDateAmt(), Loan.dueAndPaidMatching()
		Loan.readInDateAmt();
		Loan.dueAndPaidMatching();
		//step 4, Loan.adjustedFulfillmentRate()
		Loan.adjustedFulfillmentRate(beginningRunDate, endingRunDate);
		//step 5, Loan.calculatePAR(), calculateDelinquencyRate()
		Loan.calculatePAR(beginningRunDate, endingRunDate);
		Loan.calculateDelinquencyRate(beginningRunDate, endingRunDate);
        //Ended.
		} 
	
	//console interface
	public static void main(String[] args) {
		//Running from beginning date, which is the first pay date among the whole batch of loans till the designated
		//ending date that concludes this monitoring (i.e. wholly refreshing, instead of delta processing, 
		//delta-ing it would be in next development schedule), this process populates each and every column on the 
		//historical_risk_index table for next stage of reporting and modeling. 
		//Caveat: Follow steps below to run, these steps are continuous, indivisible and sequential, 
		//stopping halfway or executing them in illegal order would cause programme error and database tables in undefined, incorrect state.
				
		//Starting...
		//Step 1, PaymentProcessor.paymentScheduleOutLaying()
		PaymentProcessor.paymentScheduleOutLaying();
		@SuppressWarnings("deprecation")
		//designate the ending date, beginning date is extracted from the portfolio's payment table, eye-balling it at the moment of execution. 
		Date beginningRunDate=new Date("10/18/2015");
		Date endingRunDate=new Date("03/01/2016");
		//step 2, PaymentProcessor.fulfillmentRate()
		PaymentProcessor.fulfillmentRate(beginningRunDate,endingRunDate);
		//step 3, Loan.readInDateAmt(), Loan.dueAndPaidMatching()
		Loan.readInDateAmt();
		Loan.dueAndPaidMatching();
		//step 4, Loan.adjustedFulfillmentRate()
		Loan.adjustedFulfillmentRate(beginningRunDate, endingRunDate);
		//step 5, Loan.calculatePAR(), calculateDelinquencyRate()
		Loan.calculatePAR(beginningRunDate, endingRunDate);
		Loan.calculateDelinquencyRate(beginningRunDate, endingRunDate);
        //the end.
	}
}
