from twilio.rest import Client

def send(msgTo,msgFrom,body):  
  account_sid = "AC1f1c0939c8b365bc1136587acf771d80"
  auth_token = "642aa75c585c51810887852a2095f152"
  client = Client(account_sid, auth_token)
  client.messages.create(
    to=msgTo,
    from_=msgFrom,
    body=body)

send("+19313491084","+19142299325","test")