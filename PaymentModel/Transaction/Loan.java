package Transaction;

/*
 *To match due date and paid date and calculate paid amount then join both onto payment schedule. Usually the monthly installment payment txn
 * are coming in separate forms than payment schedule (which is usually generated natively from loan_No, originate_date, rate and loan_amt),
 * the paid amount and paid date need to be calculated, and again, it is no easy work.     
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.sql.Statement;

import Transaction.Loan.ParStatus;

class ParStatistics {
	public Date originateDate = null;
	String loan_id;
	double loanReceivable;
	double remainingReceivable;
	double loanPaid;
	int paidThroughPeriod;
	ParStatus PARFlag;

	public ParStatistics(String loan_id, double loanReceivable,
			double remainingReceivable, Date originateDate,
			int paidThroughPeriod, ParStatus PARFlag, double loanPaid) {
		this.loan_id = loan_id;
		this.remainingReceivable = remainingReceivable;
		this.loanReceivable = loanReceivable;
		this.paidThroughPeriod = paidThroughPeriod;
		this.loanPaid = loanPaid;
		this.PARFlag = PARFlag;
		this.originateDate = originateDate;
	}
}

public class Loan {
	String loan_id = new String();
	static Date originateDate = new Date();
	static Date fstPaymentDate = new Date();
	Date runDate = new Date();
	static Calendar cal = Calendar.getInstance();

	static String url = null;
	static String user = null;
	static String password = null;
	static String sql = null;
	static Connection conn = null;

	Map<Date, Double> dueDateAmt = new HashMap<Date, Double>();
	Map<Date, Double> paidDateAmt = new HashMap<Date, Double>();

	Loan(String loan_id, Date runDate, Map<Date, Double> dueDateAmt,
			Map<Date, Double> paidDateAmt) {
		this.loan_id = loan_id;
		this.runDate = runDate;
		this.paidDateAmt = paidDateAmt;
		this.dueDateAmt = dueDateAmt;
	}

	static ArrayList<Date> dateStorageFirst = new ArrayList<Date>();
	static ArrayList<Date> dateStorageSecond = new ArrayList<Date>();
	static ArrayList<DateFormatted> dueOrPaidAxel = new ArrayList<DateFormatted>();
	static Queue<DateFormatted> dueQueue = new LinkedList<DateFormatted>();
	static Queue<DateFormatted> paidQueue = new LinkedList<DateFormatted>();
	static HashMap<Date, Date> dueAndPaidDateMapping = new HashMap<Date, Date>();
	static ArrayList<Loan> al_loan = new ArrayList<Loan>();
	static ArrayList<RType> al;
	
	static void readInDateAmt() {
		String sql_paid = "select loan_id, pay_date, pay_amt from payment where loan_id=?";
		String sql_due = "select loan_id, payment_date, monthly_payment from payment_schedule";
		Statement stmt = null;
		PreparedStatement paid_ps = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("driver error");
			e.printStackTrace();
		}
		try {
			url = "jdbc:mysql://localhost/Payment_Model?profileSQL=false&&traceProtocol=false&&paranoid=false";
			// 简单写法：url
			// ="jdbc:myqsl://localhost/test(数据库名)? user=root(用户)&password=dfg353434(密码)";
			user = "root";
			password = "";
			conn = DriverManager.getConnection(url, user, password);
			stmt = conn.createStatement();
			paid_ps = conn.prepareStatement(sql_paid);
		} catch (SQLException e) {
			System.out.println("connection error.");
			e.printStackTrace();
		}

		String loan_id = null;
		String loan_id_keep = null;
		Date dueDate;
		double dueAmt;
		Loan loan = new Loan(loan_id, null, null, null);
		al_loan.clear();
		try {
			ResultSet rs = stmt.executeQuery(sql_due);
			while (rs.next()) {
				loan_id_keep = loan_id;
				loan_id = rs.getString("loan_id");
				if (!loan_id.equals(loan_id_keep)) {
					if (loan.paidDateAmt != null && loan.paidDateAmt.size() > 0) {
						al_loan.add(loan);
					}
					loan = new Loan(loan_id, null, new HashMap<Date, Double>(),
							new HashMap<Date, Double>());
					paid_ps.setString(1, loan_id);
					ResultSet rs_paid = paid_ps.executeQuery();
					while (rs_paid.next()) {
						Date d = rs_paid.getDate("pay_date");
						double amt = rs_paid.getDouble("pay_amt");
						loan.paidDateAmt.put(d, amt);
					}
				}
				if (rs.isLast()) {
					loan_id = rs.getString("loan_id");
					if (loan.paidDateAmt != null && loan.paidDateAmt.size() > 0) {
						al_loan.add(loan);
						System.out.println("loan: " + loan.loan_id);
					}

				}

				dueDate = rs.getDate("payment_date");
				dueAmt = rs.getDouble("monthly_payment");
				loan.loan_id = loan_id;
				loan.dueDateAmt.put(dueDate, dueAmt);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// conn.commit();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// System.out.println("paidDateAmt: "+ paidDateAmt.size());
	}

	static void dueAndPaidMatching() {
		ArrayList<RType> al = new ArrayList<RType>();
		ArrayList<Double> counstructingPaidAmtAccordingToDue = new ArrayList<Double>();
		ArrayList<Date> counstructingPaidDateAccordingToDue = new ArrayList<Date>();
		ArrayList<Date> counstructingDueDateAccordingToPaid = new ArrayList<Date>();
		for (Loan loan : al_loan) {
			if (loan.dueDateAmt.size() > 0) {
				dateStorageFirst.clear();
				for (Map.Entry<Date, Double> e : loan.dueDateAmt.entrySet()) {
					// System.out.println("e: "+e.getKey() );
					// System.out.println("e 2: "+e.getValue() );
					dateStorageFirst.add(e.getKey());
				}
			}
			if (loan.paidDateAmt.size() > 0) {
				dateStorageSecond.clear();
				for (Map.Entry<Date, Double> e : loan.paidDateAmt.entrySet()) {
					dateStorageSecond.add(e.getKey());
				}
				// System.out.println("e: "+e.getKey() );
			}
			Collections.sort(dateStorageFirst);// due dates
			Collections.sort(dateStorageSecond);// paid dates
			double dueSoFar = 0.0;
			double PaidSoFar = 0.0;
			DateFormatted leftOver = null;
			dueOrPaidAxel.clear();
			dueQueue.clear();
			paidQueue.clear();
			for (Date d_due : dateStorageFirst) {
				dueSoFar += loan.dueDateAmt.get(d_due);
				DateFormatted df = new DateFormatted(d_due, "due", dueSoFar);
				dueQueue.offer(df);
				System.out.println("loan id: " + loan.loan_id
						+ ", dueQueue offered: date: " + df.dueOrPaidDate
						+ ", sofar: " + dueSoFar);
			}

			for (Date d_paid : dateStorageSecond) {
				PaidSoFar += loan.paidDateAmt.get(d_paid);
				DateFormatted df = new DateFormatted(d_paid, "paid", PaidSoFar);
				paidQueue.offer(df);
				System.out.println("loan id: " + loan.loan_id
						+ ", paidQueue offered: date: " + df.dueOrPaidDate
						+ ", sofar: " + PaidSoFar);
			}
			counstructingDueDateAccordingToPaid.clear();
			counstructingPaidDateAccordingToDue.clear();
			@SuppressWarnings("deprecation")
			Date dummy_1999_Halloween = new Date(99, 10, 01);
			while (!paidQueue.isEmpty() && !dueQueue.isEmpty()) {
				// System.out.println("Due date payment amt: "+dueQueue.peek().positionAtPaymentAxel);
				if (paidQueue.size() != 0 && dueQueue.size() != 0) {
					if (paidQueue.peek().positionAtPaymentAxel < dueQueue
							.peek().positionAtPaymentAxel) {
						counstructingDueDateAccordingToPaid
								.add(dueQueue.peek().dueOrPaidDate);
						// counstructingPaidDateAccordingToDue.add(paidQueue.poll().dueOrPaidDate);
						leftOver = paidQueue.poll();
						// paidQueue.remove();
						counstructingPaidDateAccordingToDue
								.add(dummy_1999_Halloween);

					} else if (paidQueue.peek().positionAtPaymentAxel == dueQueue
							.peek().positionAtPaymentAxel) {
						counstructingDueDateAccordingToPaid
								.add(dueQueue.poll().dueOrPaidDate);
						DateFormatted f = paidQueue.peek();
						counstructingPaidDateAccordingToDue.add(paidQueue
								.poll().dueOrPaidDate);
						for (Date p : counstructingPaidDateAccordingToDue) {
							if (p.equals(dummy_1999_Halloween)) {
								counstructingPaidDateAccordingToDue.set(
										counstructingPaidDateAccordingToDue
												.indexOf(p), f.dueOrPaidDate);
							}
						}
					} else {
						counstructingDueDateAccordingToPaid
								.add(dueQueue.poll().dueOrPaidDate);
						DateFormatted f = paidQueue.peek();
						counstructingPaidDateAccordingToDue.add(paidQueue
								.peek().dueOrPaidDate);
						for (Date p : counstructingPaidDateAccordingToDue) {
							if (p.equals(dummy_1999_Halloween)) {
								counstructingPaidDateAccordingToDue.set(
										counstructingPaidDateAccordingToDue
												.indexOf(p), f.dueOrPaidDate);
							}
						}
					}
				} // if ends here
			}

			for (Date p : counstructingPaidDateAccordingToDue) {
				if (p.equals(dummy_1999_Halloween)) {
					counstructingPaidDateAccordingToDue.set(
							counstructingPaidDateAccordingToDue.indexOf(p),
							leftOver.dueOrPaidDate);
				}
			}
			for (Date df : counstructingPaidDateAccordingToDue) {
				System.out.println("loan id: " + loan.loan_id
						+ ", counstructingPaidDateAccordingToDue: date: " + df);
			}
			if (dateStorageFirst.size() > 0) {
				double sumOfPaidAmt = 0.0;
				System.out.println("dateStorageSecond: " + dateStorageSecond);
				for (Date d_paid : dateStorageSecond) {
					sumOfPaidAmt += loan.paidDateAmt.get(d_paid);
				}
				counstructingPaidAmtAccordingToDue.clear();

				for (Date d : dateStorageFirst) {
					if (sumOfPaidAmt > loan.dueDateAmt.get(d)) {
						counstructingPaidAmtAccordingToDue.add(loan.dueDateAmt
								.get(d));
					} else {
						if (sumOfPaidAmt <= 0)
							break;
						counstructingPaidAmtAccordingToDue.add(sumOfPaidAmt);
					}
					sumOfPaidAmt = sumOfPaidAmt - loan.dueDateAmt.get(d);
				}
				System.out.println("loan_id paid amt: " + loan.loan_id);
				for (Double a : counstructingPaidAmtAccordingToDue)
					System.out.println("counstructingPaidAmtAccordingToDue: "
							+ a);
				int cnt = 0;
				for (double cpaatd : counstructingPaidAmtAccordingToDue) {
					RType rInstance = new RType(loan.loan_id,
							dateStorageFirst.get(cnt),
							counstructingPaidDateAccordingToDue.get(cnt),
							cpaatd);
					al.add(rInstance);
					System.out.println("cpaatd: " + cpaatd);
					cnt++;
				}
			}
		}

		try {
			RType.writeAllMatchingIntoDB(al);
			System.out.println("$_$");
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("+++++++++++++++++++++++++++++++++");
		int k = 1;
		for (RType a : al) {
			System.out.println("loan_No: " + a.loan_id);
			System.out.println("due at: " + a.dueDate);
			System.out.println("paid at: " + a.paidDate);
			System.out.println("paid amt: " + a.paid);
			k++;
			if (k >= 20)
				break;
		}
	}

	static void adjustedFulfillmentRate(Date beginningRunDate,
			Date endingRunDate) {

		Calendar cal = Calendar.getInstance();
		double adjustedTotalReceivable = 0.0, adjustedTotalPayment = 0.0, adjustedFulfillmentRate = 0.0;
		PreparedStatement pstmt_receivable = null, sql_paid = null, pstmt_risk_index = null;
		Connection conn = null;
		ResultSet rs = null;
		String url = null;
		String user = null;
		String password = null;
		java.sql.Date sqlBeginningRunDate = new java.sql.Date(
				beginningRunDate.getTime());
		java.sql.Date sqlEndingRunDate = new java.sql.Date(
				endingRunDate.getTime());
		java.sql.Date sqlRunDate = sqlBeginningRunDate;
		cal.setTime(beginningRunDate);

		try {
			Class.forName("com.mysql.jdbc.Driver"); //
		} catch (ClassNotFoundException e) {
			System.out.println("driver error");
			e.printStackTrace();
		}
		try {
			url =
			// "jdbc:mysql://localhost/delinq_db?useUnicode=true&&characterEncoding=gb2312&autoReconnect = true";
			"jdbc:mysql://localhost/Payment_Model?profileSQL=false&&"
					+ "traceProtocol=false&&useUnicode=true&&characterEncoding=gb2312&autoReconnect = true";
			// 简单写法：url =
			// "jdbc:myqsl://localhost/test(数据库名)? user=root(用户)&password=dfg35(密码)";
			user = "root";
			password = "";
			conn = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			System.out.println("connection error.");
			e.printStackTrace();
		}
		while (sqlRunDate.before(sqlEndingRunDate)
				|| sqlRunDate.equals(sqlEndingRunDate)) {
			adjustedTotalReceivable = 0.0;
			adjustedTotalPayment = 0.0;
			adjustedFulfillmentRate = 0.0;
			try {
				String sql_receivable = "select loan_id, receivable, receivable_date from payment_paid where receivable_date<= ?;";
				String paid = "select paid, paid_date from payment_paid where paid_date<= ? and receivable_date<=?;";
				String update_sql = "update historical_risk_index set adjusted_fulfillment_rate=? where runDate=?";
				pstmt_receivable = conn.prepareStatement(sql_receivable);
				pstmt_receivable.setDate(1, sqlRunDate);
				sql_paid = conn.prepareStatement(paid);
				sql_paid.setDate(1, sqlRunDate);
				sql_paid.setDate(2, sqlRunDate);
				rs = pstmt_receivable.executeQuery();
				pstmt_risk_index = conn.prepareStatement(update_sql);
				while (rs.next()) {
					double monthlyPayment = rs.getDouble("receivable");
					adjustedTotalReceivable += monthlyPayment;
				}
				rs = null;
				rs = sql_paid.executeQuery();
				while (rs.next()) {
					double payAmt = rs.getDouble("paid");
					adjustedTotalPayment += payAmt;
				}
				adjustedFulfillmentRate = (adjustedTotalReceivable - adjustedTotalPayment)
						/ adjustedTotalReceivable;
				pstmt_risk_index.setDouble(1, adjustedFulfillmentRate);
				pstmt_risk_index.setDate(2, sqlRunDate);
				pstmt_risk_index.execute();
				cal.add(Calendar.DATE, 1);
				sqlRunDate = new java.sql.Date(cal.getTime().getTime());
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
			}
		}
		return;
	}

	public enum ParStatus {
		Current, _30D, _60D, _90D, _90DPlus
	}

	static void calculatePAR(Date beginningRunDate, Date endingRunDate) {
		// calculate Portfolio-at-Risk, i.e. Asset-at-Risk.
		// 1. Calculate Remaining Receivable from Loan and Payment table.
		// 2. Determine PAR Status by PaymentPaid table.
		// 3. Calculate PAR.
		// 4. Calculate Historical PAR and Update in Historical Risk Index
		// table.
		java.sql.Date sqlBeginningRunDate = new java.sql.Date(
				beginningRunDate.getTime());
		java.sql.Date sqlEndingRunDate = new java.sql.Date(
				endingRunDate.getTime());
		java.sql.Date sqlRunDate = sqlBeginningRunDate;
		Calendar calendar = Calendar.getInstance();
		cal.setTime(beginningRunDate);

		double loanReceivable = 0, remainingReceivable = 0, allReceivable = 0, allPaid = 0, loanPaid = 0, allRemainingReceivable = 0, currentRemainingReceivable = 0, _30D_remainingReceivable = 0, _60D_remainingReceivable = 0, _90D_remainingReceivable = 0, _90DPlus_remainingReceivable = 0, current_PAR = 0, _30D_PAR = 0, _60D_PAR = 0, _90D_PAR = 0, _90Dplus_PAR = 0;
		int paidThroughPeriod = 0, currentPeriod = 0;
		PreparedStatement stmt = null, payment = null, paidThru = null, currentPeriodCount = null, paid = null, update_risk_index = null;
		ParStatus PARFlag = null; // PAR flg.
		Map<String, ParStatistics> parStatisticsMap = new HashMap<String, ParStatistics>();
		Set<String> loan_key;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			url = "jdbc:mysql://localhost/Payment_Model?profileSQL=false&&traceProtocol=false&&paranoid=false";
			user = "root";
			password = "";
			conn = DriverManager.getConnection(url, user, password);
		} catch (Exception e) {
			System.out.println("driver error");
			e.printStackTrace();
		}
		try {
			String sql_loan = "select loan_no, annual_rate, installments, originate_date, fst_payment_date, loan_amt, "
					+ "amortization_type from loan where originate_date<=?;";
			String sql_payment = "select loan_id, pay_date, pay_amt from payment where loan_id=? and pay_date<=?;";
			// String sql_paidThrough =
			// "select loan_id, receivable_date, paid from payment_paid where loan_id=? "
			// +
			// "and paid is not null and paid=receivable and paid_date <=? order by receivable_date;";
			String sql_paidThrough = "select loan_id, max(pay_period) pay_pd from payment_paid where loan_id=? "
					+ "and paid is not null and paid=receivable and paid_date <=? and receivable_date <=? group by loan_id order by receivable_date;";
			String sql_current_period = "select loan_id, max(pay_period) pay_pd from payment_paid where loan_id=? "
					+ "and receivable_date<=? group by loan_id";
			String sql_paid = "select loan_id, pay_date, pay_amt from payment where loan_id=? "
					+ " and pay_date<=?;";
			String update_sql = "update historical_risk_index set current_PAR=?, 30D_PAR=?, 60D_PAR=?, 90D_PAR=?, 90Dplus_PAR=? where runDate=?";
			stmt = conn.prepareStatement(sql_loan);
			payment = conn.prepareStatement(sql_payment);
			paidThru = conn.prepareStatement(sql_paidThrough);
			currentPeriodCount = conn.prepareStatement(sql_current_period);
			paid = conn.prepareStatement(sql_paid);
			update_risk_index = conn.prepareStatement(update_sql);

			while (sqlRunDate.before(sqlEndingRunDate)
					|| sqlRunDate.equals(sqlEndingRunDate)) {
				allPaid = 0;
				allReceivable = 0;
				// loanPaid = 0;
				currentRemainingReceivable = 0;
				_30D_remainingReceivable = 0;
				_60D_remainingReceivable = 0;
				_90D_remainingReceivable = 0;
				_90DPlus_remainingReceivable = 0;
				stmt.setDate(1, sqlRunDate);
				ResultSet rs = stmt.executeQuery();
				String loan_id = null;
				String amortizationType = null;
				double installments, rate, loanAmt;
				Date originateDate = null, fst_payment_date = null;
				ParStatistics stats;
				while (rs.next()) {
					loanReceivable = 0;
					remainingReceivable = 0;
					loanPaid = 0;
					originateDate = null;
					fst_payment_date = null;
					paidThroughPeriod = 0;
					currentPeriod = 0;
					loan_id = rs.getString("loan_no");
					System.out.println(" loan_id: " + loan_id);
					amortizationType = rs.getString("amortization_type");
					installments = rs.getDouble("installments");
					originateDate = rs.getDate("originate_date");
					fst_payment_date = rs.getDate("fst_payment_date");
					loanAmt = rs.getDouble("loan_amt");
					rate = rs.getDouble("annual_rate") / 100;
					if (amortizationType.equals("先息后本"))
						loanReceivable = loanAmt * rate * installments / 12
								+ loanAmt;
					else if (amortizationType.equals("等额本息"))
						loanReceivable = loanAmt * (rate / 12) * installments
								* Math.pow((1 + rate / 12), installments)
								/ (Math.pow((1 + rate / 12), installments) - 1);
					payment.setString(1, loan_id);
					payment.setDate(2, sqlRunDate);
					ResultSet rs2 = payment.executeQuery();
					remainingReceivable = loanReceivable;
					while (rs2.next()) {
						remainingReceivable -= rs2.getDouble("pay_amt");
					}
					paidThru.setString(1, loan_id);
					paidThru.setDate(2, sqlRunDate);
					paidThru.setDate(3, sqlRunDate);
					ResultSet rs3 = paidThru.executeQuery();
					currentPeriodCount.setString(1, loan_id);
					currentPeriodCount.setDate(2, sqlRunDate);
					ResultSet rs5 = currentPeriodCount.executeQuery();
					calendar.setTime(fst_payment_date);
					calendar.add(Calendar.MONTH, -1);
					PARFlag = null;
					while (rs3.next()) {
						paidThroughPeriod = rs3.getInt("pay_pd");
					}
					while (rs5.next()) {
						currentPeriod = rs5.getInt("pay_pd");
					}

					System.out.println(" sqlRunDate : " + sqlRunDate);

					System.out.println(" paidThroughPeriod : "
							+ paidThroughPeriod);

					System.out.println(" currentPeriod : " + currentPeriod);
					if (currentPeriod - paidThroughPeriod == 0)
						PARFlag = ParStatus.Current;
					else if (currentPeriod - paidThroughPeriod == 1)
						PARFlag = ParStatus._30D;
					else if (currentPeriod - paidThroughPeriod == 2)
						PARFlag = ParStatus._60D;
					else if (currentPeriod - paidThroughPeriod == 3)
						PARFlag = ParStatus._90D;
					else if (currentPeriod - paidThroughPeriod > 3)
						PARFlag = ParStatus._90DPlus;
					System.out.println("PARFlag: " + PARFlag);
					paid.setString(1, loan_id);
					paid.setDate(2, sqlRunDate);
					ResultSet rs4 = paid.executeQuery();
					while (rs4.next()) {
						loanPaid += rs4.getDouble("pay_amt");
					}
					stats = new ParStatistics(loan_id, loanReceivable,
							remainingReceivable, originateDate,
							paidThroughPeriod, PARFlag, loanPaid);
					parStatisticsMap.put(loan_id, stats);
				}
				loan_key = parStatisticsMap.keySet();
				for (String key : loan_key) {
					{
						allReceivable += parStatisticsMap.get(key).loanReceivable;
						allPaid += parStatisticsMap.get(key).loanPaid;
					}
					if (parStatisticsMap.get(key).PARFlag != null) {
						if (parStatisticsMap.get(key).PARFlag
								.equals(ParStatus.Current))
							currentRemainingReceivable += parStatisticsMap
									.get(key).remainingReceivable;
						if (parStatisticsMap.get(key).PARFlag
								.equals(ParStatus._30D))
							_30D_remainingReceivable += parStatisticsMap
									.get(key).remainingReceivable;
						if (parStatisticsMap.get(key).PARFlag
								.equals(ParStatus._60D))
							_60D_remainingReceivable += parStatisticsMap
									.get(key).remainingReceivable;
						if (parStatisticsMap.get(key).PARFlag
								.equals(ParStatus._90D))
							_90D_remainingReceivable += parStatisticsMap
									.get(key).remainingReceivable;
						if (parStatisticsMap.get(key).PARFlag
								.equals(ParStatus._90DPlus))
							_90DPlus_remainingReceivable += parStatisticsMap
									.get(key).remainingReceivable;
					}
				}
				allRemainingReceivable = allReceivable - allPaid;
				current_PAR = currentRemainingReceivable
						/ allRemainingReceivable;
				_30D_PAR = _30D_remainingReceivable / allRemainingReceivable;
				_60D_PAR = _60D_remainingReceivable / allRemainingReceivable;
				_90D_PAR = _90D_remainingReceivable / allRemainingReceivable;
				_90Dplus_PAR = _90DPlus_remainingReceivable
						/ allRemainingReceivable;
				update_risk_index.setDouble(1, current_PAR);
				update_risk_index.setDouble(2, _30D_PAR);
				update_risk_index.setDouble(3, _60D_PAR);
				update_risk_index.setDouble(4, _90D_PAR);
				update_risk_index.setDouble(5, _90Dplus_PAR);
				update_risk_index.setDate(6, sqlRunDate);
				update_risk_index.execute();
				cal.setTime(sqlRunDate);
				cal.add(Calendar.DATE, 1);
				sqlRunDate = new java.sql.Date(cal.getTime().getTime());
				parStatisticsMap.clear();
			}
		} catch (Exception e) {
			System.out.println("connection error.");
			e.printStackTrace();
		}
	}

	static void calculateDelinquencyRate(Date beginningRunDate,
			Date endingRunDate) {
		double Rec_Interest = 0, Paid_MonthlyPayment, rec_MonthlyPayment, paid_interest, paid_principal, 
				rec_interest, rec_principal, paid_remainingPrincipal, rec_remainingPrincipal, 
				increment_paid = 0, increment_monthlyPayment = 0, denominator, numeratorSigmaCurrent,
				numeratorSigma1MD , numeratorSigma2MD, numeratorSigma3MD, numeratorSigma3MDPlus;
		int del_payThroughPeriod, del_currentPeriod;
		Map<String, Integer> map_currentPeriod= new HashMap<String, Integer>(); 
		Map<String, Integer> map_payThroughPeriod = new HashMap<String, Integer>();
		Map<String, Subtraction> map_PAR = new HashMap<String, Subtraction>();
		String loan_id, PAR_flag;
		int pay_period;
		java.sql.Date paid_date, receivable_date;
		ResultSet rs_increment = null, rs_receivable = null, rs_tmp=null;
		try {
			Class.forName("com.mysql.jdbc.Driver"); //
		} catch (ClassNotFoundException e) {
			System.out.println("driver error");
			e.printStackTrace();
		}
		try {
			url = "jdbc:mysql://localhost/Payment_Model?profileSQL=false&&"
					+ "traceProtocol=false&&useUnicode=true&&characterEncoding=gb2312&autoReconnect = true";
			user = "root";
			password = "";
			conn = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			System.out.println("connection error.");
			e.printStackTrace();
		}
		Statement stmt;
		PreparedStatement paidStmt, receivableStmt, updateStmt, groupUpdateStmt, PARStmt1,PARStmt2,PARStmt3,
		loanAmountDenominatorStmt, loanAmountNumeratorStmt,updateDelRateStmt;
		String truncate_sql = "truncate temp_del_paymentpaid;";
		String populate_sql = "insert into temp_del_paymentpaid (loan_id, Pay_Period,Paid_date,"
				+ "Paid_MonthlyPayment,Rec_MonthlyPayment,Receivable_Date,Rec_Interest,Rec_RemainingPrincipal,Rec_Principal)"
				+ "select s.loan_id, s.pay_period Pay_Period, p.paid_date Paid_date, p.paid Paid_MonthlyPayment,"
				+ "s.monthly_payment Rec_MonthlyPayment, p.receivable_date ReceivableDate, s.interest Rec_Interest,"
				+ " s.ending_principal Rec_RemainingPrincipal, s.principal_paydown Rec_Principal "
				+ "from payment_paid p join payment_schedule s on p.loan_id=s.loan_id and p.pay_period=s.pay_period";
		String select_sql = "select * from temp_del_paymentpaid;";
		String select_sql2 = "select * from del_paymentpaid;";
		String select_sql3 = "select max(pay_period) del_payThroughPeriod, loan_id from del_paymentpaid where receivable_date<= ? "
				+ "and paid_date<=? and rec_monthlyPayment=paid_MonthlyPayment group by loan_id; ";
		String select_sql4="select max(pay_period) del_currentPeriod, loan_id from del_paymentpaid "
				+ "where receivable_date<= ? group by loan_id";
		String select_sql5 = "select loan_no from loan where originate_date<=?;";
		/*
		String select_sql5="select case (b.del_currentPeriod - a.del_payThroughPeriod ) when 0 then 'Current'"
				+ "when 1 then '1MD' when 2 then '2MD' when 3 then '3MD' else '3MDPlus' end as PAR_flag, a.loan_id from "
				+ "(select max(pay_period) del_payThroughPeriod, loan_id from del_paymentpaid where receivable_date<= ? "
				+ "and paid_date<=? and rec_monthlyPayment=paid_MonthlyPayment group by loan_id) a join "
				+ "(select max(pay_period) del_currentPeriod, loan_id from del_paymentpaid "
				+ "where receivable_date<= ? group by loan_id) b on a.loan_id=b.loan_id;";
		*/
		String increment_paid_sql = "select sum(pay_amt) increment_paid from payment where loan_id=? and pay_date <=?";
		// group by loan_id
		String increment_receivable_sql = "select sum(Rec_MonthlyPayment) increment_MonthlyPayment from temp_del_paymentpaid "
				+ " where loan_id=? and receivable_date <=?";
		String update_sql = "update temp_del_paymentpaid set paid_interest=?, paid_principal=?, "
				+ "paid_remainingPrincipal=? where loan_id=? and pay_period=?";
		String move_sql = "insert into del_paymentpaid select * from temp_del_paymentpaid;";
		String group_update_sql = "update del_paymentpaid set paid_date=?, Paid_MonthlyPayment=?, "
				+ "paid_principal=?, paid_interest=?, paid_remainingPrincipal=? where loan_id=? and pay_period>?";
		String loanAmountDenominator= "select sum(loan_amt) denominator from loan where originate_date<=?";
		String loanAmountNumerator= "select sum(loan_amt) numerator from loan where originate_date<=? and loan_no=?";
		String update_del_rate_sql = "update historical_risk_index set current_delRate=?, 1MD_delRate=?,"
				+ "2MD_delRate=?,3MD_delRate=?,3MDPlus_delRate=? where runDate=?";
	
		try {
			stmt = conn.createStatement();
			stmt.execute(truncate_sql);
			stmt.execute(populate_sql);
			ResultSet rs = stmt.executeQuery(select_sql);
			paidStmt = conn.prepareStatement(increment_paid_sql);
			receivableStmt = conn.prepareStatement(increment_receivable_sql);
			updateStmt = conn.prepareStatement(update_sql);
			groupUpdateStmt = conn.prepareStatement(group_update_sql);
			PARStmt1 = conn.prepareStatement(select_sql3);
			PARStmt2 = conn.prepareStatement(select_sql4);
			PARStmt3 = conn.prepareStatement(select_sql5);
			loanAmountDenominatorStmt=conn.prepareStatement(loanAmountDenominator);
			loanAmountNumeratorStmt=conn.prepareStatement(loanAmountNumerator);
			updateDelRateStmt=conn.prepareStatement(update_del_rate_sql);
			/*while (rs.next()) {
				loan_id = rs.getString("loan_id");
				pay_period = rs.getInt("pay_period");
				Rec_Interest = rs.getDouble("Rec_Interest");
				Paid_MonthlyPayment = rs.getDouble("Paid_MonthlyPayment");
				paid_date = rs.getDate("paid_date");
				receivable_date = rs.getDate("receivable_date");
				paid_interest = (Paid_MonthlyPayment >= Rec_Interest) ? Rec_Interest
						: Paid_MonthlyPayment;
				paid_principal = (Paid_MonthlyPayment >= Rec_Interest) ? Paid_MonthlyPayment
						- paid_interest
						: 0;
				paidStmt.setString(1, loan_id);
				paidStmt.setDate(2, paid_date);
				receivableStmt.setString(1, loan_id);
				receivableStmt.setDate(2, receivable_date);
				rs_increment = paidStmt.executeQuery();
				rs_receivable = receivableStmt.executeQuery();
				while (rs_increment.next()) {
					increment_paid = rs_increment.getDouble("increment_paid");
				}
				while (rs_receivable.next()) {
					increment_monthlyPayment = rs_receivable
							.getDouble("increment_MonthlyPayment");
				}
				paid_remainingPrincipal = increment_paid
						- increment_monthlyPayment;
				updateStmt.setDouble(1, paid_interest);
				updateStmt.setDouble(2, paid_principal);
				updateStmt.setDouble(3, paid_remainingPrincipal);
				updateStmt.setString(4, loan_id);
				updateStmt.setInt(5, pay_period);
				updateStmt.execute();
			}
			stmt.execute(move_sql);
			rs.close();
			rs = stmt.executeQuery(select_sql2);
			while (rs.next()) {
				loan_id = rs.getString("loan_id");
				pay_period = rs.getInt("pay_period");
				paid_remainingPrincipal = rs
						.getDouble("paid_remainingPrincipal");
				rec_remainingPrincipal = rs.getDouble("rec_remainingPrincipal");
				rec_MonthlyPayment = rs.getDouble("rec_MonthlyPayment");
				paid_date = rs.getDate("paid_date");
				receivable_date = rs.getDate("receivable_date");
				rec_principal = rs.getDouble("rec_principal");
				rec_interest = rs.getDouble("rec_interest");
				if (paid_remainingPrincipal >= rec_remainingPrincipal
						&& (paid_date.before(receivable_date) || paid_date
								.equals(receivable_date))) {
					groupUpdateStmt.setDate(1, paid_date);
					groupUpdateStmt.setDouble(2, rec_MonthlyPayment);
					groupUpdateStmt.setDouble(3, rec_principal);
					groupUpdateStmt.setDouble(4, rec_interest);
					groupUpdateStmt.setDouble(5, rec_remainingPrincipal);
					groupUpdateStmt.setString(6, loan_id);
					groupUpdateStmt.setInt(7, pay_period);
					groupUpdateStmt.execute();
				}
			}*/
			rs.close();
			java.sql.Date sqlBeginningRunDate = new java.sql.Date(
					beginningRunDate.getTime());
			java.sql.Date sqlEndingRunDate = new java.sql.Date(
					endingRunDate.getTime());
			java.sql.Date sqlRunDate = sqlBeginningRunDate;
			cal.setTime(beginningRunDate);

			while (sqlRunDate.before(sqlEndingRunDate)
					|| sqlRunDate.equals(sqlEndingRunDate)) {
				PARStmt1.setDate(1, sqlRunDate);
				PARStmt1.setDate(2, sqlRunDate);
				PARStmt2.setDate(1, sqlRunDate);
				PARStmt3.setDate(1, sqlRunDate);
				rs = PARStmt1.executeQuery();	
				while (rs.next()){
					loan_id=rs.getString("loan_id");
					del_payThroughPeriod=rs.getInt("del_payThroughPeriod");		
					map_payThroughPeriod.put(loan_id, del_payThroughPeriod);					
				}
				rs.close();
				rs=PARStmt2.executeQuery();	
				while (rs.next()){
					loan_id=rs.getString("loan_id");
					del_currentPeriod=rs.getInt("del_currentPeriod");		
					map_currentPeriod.put(loan_id, del_currentPeriod);					
				}
				rs.close();
				rs=PARStmt3.executeQuery();	
				while (rs.next()){
					loan_id=rs.getString("loan_no");
					map_PAR.put(loan_id, new Subtraction());					
				}
				for (String k: map_PAR.keySet()){
					map_PAR.get(k).setSubtractor((map_payThroughPeriod.get(k)==null)? 0: map_payThroughPeriod.get(k));
					map_PAR.get(k).setSubtracted((map_currentPeriod.get(k)==null)? 0: map_currentPeriod.get(k));
					map_PAR.get(k).subtract();
				}
				
				denominator=0;
				numeratorSigmaCurrent=0;
				numeratorSigma1MD = 0;
				numeratorSigma2MD=0; 
				numeratorSigma3MD=0; 
				numeratorSigma3MDPlus=0;
				loanAmountDenominatorStmt.setDate(1, sqlRunDate);
				rs_tmp = loanAmountDenominatorStmt.executeQuery();
				rs_tmp.next();
				denominator = rs_tmp.getDouble("denominator");
				rs_tmp.close();
				for (String k: map_PAR.keySet()){
					loan_id=k;
					PAR_flag=map_PAR.get(k).PARStatus;
					rs_tmp.close();
					loanAmountNumeratorStmt.setDate(1, sqlRunDate);
					loanAmountNumeratorStmt.setString(2, loan_id);	
					rs_tmp=loanAmountNumeratorStmt.executeQuery();
					rs_tmp.next();
					if (PAR_flag.equals("Current")){
						numeratorSigmaCurrent+=rs_tmp.getDouble("numerator");
					}		
					if (PAR_flag.equals("1MD")){
						numeratorSigma1MD+=rs_tmp.getDouble("numerator");
					}					
					if (PAR_flag.equals("2MD")){
						numeratorSigma2MD+=rs_tmp.getDouble("numerator");
					}if (PAR_flag.equals("3MD")){
						numeratorSigma3MD+=rs_tmp.getDouble("numerator");
					}if (PAR_flag.equals("3MDPlus")){
						numeratorSigma3MDPlus+=rs_tmp.getDouble("numerator");
					}
				}		
				updateDelRateStmt.setDouble(1, numeratorSigmaCurrent/denominator);
				updateDelRateStmt.setDouble(2, numeratorSigma1MD/denominator);
				updateDelRateStmt.setDouble(3, numeratorSigma2MD/denominator);
				updateDelRateStmt.setDouble(4, numeratorSigma3MD/denominator);
				updateDelRateStmt.setDouble(5, numeratorSigma3MDPlus/denominator);
				updateDelRateStmt.setDate(6, sqlRunDate);
				updateDelRateStmt.execute();
				cal.setTime(sqlRunDate);
				cal.add(Calendar.DATE, 1);
				sqlRunDate = new java.sql.Date(cal.getTime().getTime());
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		@SuppressWarnings("deprecation")
		Date beginningRunDate = new Date("10/18/2015");
		@SuppressWarnings("deprecation")
		Date endingRunDate = new Date("12/31/2015");
		/*// step 3, readInDateAmt(), dueAndPaidMatching()
		readInDateAmt();
		dueAndPaidMatching();
		// step 4, adjustedFulfillmentRate()
		adjustedFulfillmentRate(beginningRunDate, endingRunDate);
		// step 5, calculatePAR(), calculateDelinquencyRate()
		calculatePAR(beginningRunDate, endingRunDate);*/
		calculateDelinquencyRate(beginningRunDate, endingRunDate);
	}
}