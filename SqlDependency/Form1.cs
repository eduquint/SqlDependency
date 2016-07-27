using System;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Windows.Forms;
using SqlDependency.Properties;

namespace SqlDependency {
	public partial class Form1 : Form {

		#region Protected Vars

		protected int ChangeCount;
		protected const string TableName = "PréBoleto";
		protected const string StatusMessage = "Changes occurred:";
		protected bool ExitRequested = false;
		protected bool WaitInProgress;

		// The following objects are reused
		// for the lifetime of the application.
		protected DataSet DataToWatch;
		protected SqlConnection Connection;
		protected SqlCommand Command;

		// The Service Name is required to correctly  register for notification.
		// The Service Name must be already defined with Service Broker for the database you are querying.
		protected const string ServiceName = "SbmDashboardChangeNotifications";

		protected const string QueueName = "SbmDashboardChangeMessages";

		// The database name is needed for both the connection string and the SqlNotificationRequest.Options property.
		protected const string DatabaseName = "SBM With Users";


		// Specify how long the notification request should wait before timing out.
		// This value waits for 30 seconds. 
		protected int NotificationTimeout = 30;

		#endregion

		#region Constructor/Destructor

		public Form1() {
			InitializeComponent();
		}

		private void Form1_FormClosed(object sender, FormClosedEventArgs e) => Connection?.Close();

		#endregion

		#region Event Processing Methods

		private void button1_Click(object sender, EventArgs e) {
			ChangeCount = 0;
			label1.Text = $"{StatusMessage} {ChangeCount}";

			if (Connection == null) {
				Connection = new SqlConnection($"Data Source=(local);Integrated Security=true;Initial Catalog={DatabaseName};Pooling=False;Asynchronous Processing=true;");
			}

			if (Command == null) {
				Command = new SqlCommand(Resources.StoreProcedure, Connection);
			}

			if (DataToWatch == null) {
				DataToWatch = new DataSet();
			}

			button1.Enabled = !checkBox1.Checked;

			GetData(checkBox1.Checked);
		}


		private void checkBox1_CheckedChanged(object sender, EventArgs e) => button1.Enabled = !checkBox1.Checked || button1.Enabled;

		#endregion

		#region Supporting Methods

		private void GetData(bool register) {
			// Empty the DataSet so that there is only one batch of data displayed.
			DataToWatch.Clear();

			// Make sure the command object does not already have	a notification object associated with it.
			Command.Notification = null;

			if (register) {
				// Create and bind the SqlNotificationRequest object to the command object.
				// If a time-out occurs, a notification will indicating that is the reason for the notification.
				var request = new SqlNotificationRequest {
																									 UserData = new Guid().ToString(),
																									 Options = $"Service={ServiceName};local database={DatabaseName}",
																									 Timeout = NotificationTimeout
																								 };

				Command.Notification = request;
			}

			using (var adapter = new SqlDataAdapter(Command)) {
				adapter.Fill(DataToWatch, TableName);

				dataGridView1.DataSource = DataToWatch;
				dataGridView1.DataMember = TableName;
			}

			if (register) {
				// Start the background listener.
				Listen();
			}
		}

		private void Listen() {
			using (var sqlCommand = new SqlCommand($"WAITFOR (RECEIVE * FROM {QueueName});", Connection)) {
				if (Connection.State != ConnectionState.Open) {
					Connection.Open();
				}

				// Make sure we don't time out before the notification request times out.
				sqlCommand.CommandTimeout = NotificationTimeout + 120;

				AsyncCallback callBack = OnReaderComplete;
				var asynResult = sqlCommand.BeginExecuteReader(callBack, sqlCommand);

				if (asynResult.IsCompleted) {
					WaitInProgress = true;
				}
			}
		}

		private void OnReaderComplete(IAsyncResult asynResult) {
			// You may not interact with the form and its contents from a different thread, and this callback procedure
			// is all but guaranteed to be running from a different thread than the form. 
			// Therefore you cannot simply call code that updates the UI.
			// Instead, you must call the procedure from the form's thread.
			// This code will use recursion to switch from the thread pool to the UI thread.
			if (InvokeRequired) {
				AsyncCallback switchThreads = OnReaderComplete;
				object[] args = {
													asynResult
												};
				BeginInvoke(switchThreads, args);
				return;
			}

			// At this point, this code will run on the UI thread.
			try {
				WaitInProgress = false;
				var reader = ((SqlCommand) asynResult.AsyncState).EndExecuteReader(asynResult);

				while (reader.Read()) {
					// Empty queue of messages.
					// Application logic could parse the queue data to determine why things.
					for (var i = 0; i <= reader.FieldCount - 1; i++)
						Debug.WriteLine(reader[i].ToString());
				}

				reader.Close();
				ChangeCount += 1;
				label1.Text = $"{StatusMessage} {ChangeCount}";
				Application.DoEvents();

				// The user can decide to request	a new notification by	checking the CheckBox on the form.
				// However, if the user has requested to exit, we need to do that instead.
				if (ExitRequested) {
					Close();
				}
				else {
					GetData(checkBox1.Checked);
				}
			}
			catch (Exception ex) {
				MessageBox.Show(ex.Message, ex.Source, MessageBoxButtons.OK, MessageBoxIcon.Warning);
			}
		}

		#endregion
	}
}