'''
Created on 26/lug/2013

@author: a530966
'''
import cx_Oracle
import sys
import os
from pymongo import Connection
from pymongo.errors import ConnectionFailure

def estrai_misuratori(i_pr, i_cur):
    res=[]
    mis={}
    i_cur.execute("""select ns.dbi_punto_key,
       ns.matricola_contatore,
       ns.dt_valido_dal,
       ns.dt_valido_al,
       ns.gruppo_misuratore_sap,
       ka.n_valore
  from work_misuratori_ee             ns,
       dbi_user.wrk_sgm_anag_k_attiva ka
 where
 ka.cd_punto = ns.dbi_punto_key
 and ns.dbi_punto_key = :p_pr
 group by 
 ns.dbi_punto_key,
       ns.matricola_contatore,
       ns.dt_valido_dal,
       ns.dt_valido_al,
       ns.gruppo_misuratore_sap,
       ka.n_valore""", p_pr = i_pr)
    for rec in i_cur:
        mis={}
        mis['matricola']        = rec[1]
        mis['d_valido_dal']     = rec[2]
        mis['d_valido_al']      = rec[3]
        mis['gruppo_num']       = rec[4]
        mis['k_misura']         = rec[5]
        res.append(mis)
    return res
        
def estrai_letture(i_pr, i_cur):
    res=[]
    let={}
    i_cur.execute("""select * from dbi_user.Wrk_Sap_Sap_Misure_Ele where id_fornitura = :p_pr""", p_pr = i_pr )
    for rec in i_cur:
        let={}
        let['d_periodo']        = rec[2]
        let['_id']              = rec[3]
        let['id_tp_misura']     = rec[4]
        let['cd_fascia']        = rec[6]
        let['consumo_attiva']   = rec[7]
        let['consumo_reattiva'] = rec[8]
        let['consumo_potenza']  = rec[9]
        let['dt_inizio_periodo']= rec[10]
        let['dt_fine_periodo']  = rec[11]
        let['lettura_attiva_1'] = rec[12]
        let['lettura_attiva_2'] = rec[13]
        let['lettura_reattiva_1'] = rec[14]
        let['lettura_reattiva_2'] = rec[15]
        let['n_k']                = rec[16]
        let['fl_sgm']            = rec[17]
        let['fl_reale']          = rec[17]       
        res.append(let)
    return res

def estrai_periodi(i_pr, i_cur, i_forn):
    res=[]
    per={}
    i_cur.execute("""select dbi_punto_key||ordine, fe.*
         from dbi_user.ifc_sap_fornitura_ee_config fe
         where fe.dbi_punto_key = :p_pr
         and fe.dbi_anubis_id_fornitura = :p_forn
         order by progressivo""", p_pr = i_pr, p_forn = i_forn) 
    for rec in i_cur:
        per={}
        per['_id']                  = rec[0]
        per['valido_dal']           = rec[3]
        per['valido_al']            = rec[4]
        per['progressivo']          = rec[5]
        per['configurazione']       = rec[6]
        per['id_distributore']      = rec[7]
        per['tensione']             = rec[13]
        per['pot_contrattuale']     = rec[15]
        per['pot_disponibile']      = rec[16]
        per['pot_impegnata']        = rec[17]
        per['data_sgm']             = rec[30]
        per['mercato']              = rec[48]
        per['mercato_versione']     = rec[49]
        per['k_energia']            = rec[22]
        per['id_fornitura']         = rec[1]
        res.append(per)
    return res
    
def estrai_indirizzi(i_pr, i_cflg9, i_cur, i_indcli):
    res = []
    ind = {}
    i_cur.execute("""SELECT b.indprog, --0
       c.indccom, --1
       c.inddcom, --2
       c.indcap, -- 3
       SUBSTR(c.indindi || ' ' || c.indccvia, 0, 60), --4
       NVL(SUBSTR(c.indcivi, 0, 10), '99999'), --5
       c.indprov, --6
       dec_regiogroup(c.indregio), --7
       DECODE(c.indnazi, 'ITALIA', 'IT', 'SAN MARINO', 'SM', 'IT'),
       c.IDFRAZIONE_GEOLAB
      FROM v_contratti v, DBI_USER.ifc_sap_anagrindirizzi_pr b, v_indirizzi c
     WHERE b.pr = :p_pr
       and v.cncon = b.pr
    AND b.indprog = c.indprog
    group by 
    b.indprog,
    c.indccom,
    c.inddcom,
    c.indcap,
    SUBSTR(c.indindi || ' ' || c.indccvia, 0, 60), --4
    NVL(SUBSTR(c.indcivi, 0, 10), '99999'), --5
    c.indprov, --6
    dec_regiogroup(c.indregio), --7
    DECODE(c.indnazi, 'ITALIA', 'IT', 'SAN MARINO', 'SM', 'IT'),
    c.IDFRAZIONE_GEOLAB
    """, p_pr = i_pr)
    for rec in i_cur:
        ind = {}
        ind['_id']      = str(rec[0])+'F'
        ind['tipo']     = 'FORNITURA'
        ind['comune']   = rec[2].decode('latin-1').encode('utf8','xmlcharrefreplace')
        ind['cap']      = rec[3]
        ind['via']      = rec[4].decode('latin-1').encode('utf8','xmlcharrefreplace')
        ind['civico']   = rec[5].decode('latin-1').encode('utf8','xmlcharrefreplace')
        res.append(ind)
    ind = {}    
    i_cur.execute("""select    a.indprog,
          indccom, 
          inddcom, 
          indindi||' '||indccvia, 
          substr(indcivi,0,10),
          indest,
          indcap,
          indprov,
          decode(indnazi,'ITALIA','IT','SAN MARINO','SM','IT'),
          indregio
          from DBI_USER.ifc_sap_anagrindirizzi a, v_indirizzi v
          where indcli = :p_cflg9
          and a.indprog = v.indprog
          group by
          a.indprog,
          indccom, 
          inddcom, 
          indindi||' '||indccvia, 
          substr(indcivi,0,10),
          indest,
          indcap,
          indprov,
          decode(indnazi,'ITALIA','IT','SAN MARINO','SM','IT'),
          indregio""", p_cflg9 = i_cflg9)
    for rec in i_cur:
        ind = {}
        ind['_id']      = str(rec[0])+'R'
        ind['tipo']     = 'RECAPITO'
        ind['comune']   = rec[2].decode('latin-1').encode('utf8','xmlcharrefreplace')
        ind['cap']      = rec[6]
        ind['via']      = rec[3].decode('latin-1').encode('utf8','xmlcharrefreplace')
        ind['civico']   = rec[4]
        res.append(ind)
    return res
        
        
def main():
    """ Connect to MongoDB """
    try:
        #c = Connection(host="212.71.255.50", port=27017)
        c = Connection(host="192.168.189.136", port=27017)
        #c = Connection(host="localhost", port=27017)
        print "Connected successfully"
    except ConnectionFailure, e:
        sys.stderr.write("Could not connect to MongoDB: %s" % e)
        sys.exit(1)
    dbm = c["sorg"]
    """ Connect to Oracle """
    username = os.environ['username']
    password = os.environ['password']
    host = os.environ['host']
    sid = os.environ['sid']
    #fileinput = os.environ['fileinput']
    conninfo = username + '/' + password + '@' + host + '/' + sid
    print conninfo

    #connessione al database
    try:
        db = cx_Oracle.connect(conninfo)
        print "------------------------------------------------------------"
        print "Benvenuto utente " + username
        print "Sei connesso a Oracle DB versione " + db.version 
        print "------------------------------------------------------------"
    except cx_Oracle.DatabaseError, exc:
        error, = exc.args
        print "------------------------------------------------------------"
        print error.message
        print "------------------------------------------------------------"
        sys.exit()
    cur = db.cursor()    
    cur1 = db.cursor()
    doc = {}
    cur.execute("""select vc.acdan,
       vc.arags,
       vc.apers,
       vc.cncon,
       vc.cserv,
       vc.cflg9,
       vc.caliv,
       vc.cmepag,
       vc.ctar1,
       vc.cpod,
       sp.stato_punto_dbi,
       vc.segmento_cliente_sap,
       sp.dbi_anubis_id_fornitura,
       vc.cdsti,
       vc.cflg4,
       delabi,
       delcab,
       delcin,
       deliban,
       cdsal,
       dbi_original_cd_prodotto
  from v_contratti                    vc
       left outer join dbi_user.ifc_sap_anagrbanche ab on (ab.delprog = vc.delprog and ab.delcli = vc.acdan and ab.delef = vc.cflg9),
       dbi_user.ifc_sap_statopunti    sp,
       dbi_user.ifc_sap_anagrcontrele ae,
       z_pr_in_lavorazione_new        zp
 where sp.cd_punto = vc.cncon
   --and sp.stato_punto_dbi = 'ATTIVO'
   and vc.ordine_reverse = 1
   and vc.cserv = 'ENERGIA'
   and zp.cncon = vc.cncon
   and ae.cncon = vc.cncon
   and ae.ordine_reverse = 1
""")
    for rec in cur:
        doc = {}
        #print rec
        w_acdan         = rec[0]
        if rec[1] <> None:
            w_ragsoc        = rec[1].decode('latin-1').encode('utf8','xmlcharrefreplace')
        w_tippers       = rec[2]
        w_cncon         = rec[3]
        w_cdservizio    = rec[4]
        w_cflg9         = rec[5]
        w_caliv         = rec[6]
        w_cmepag        = rec[7]
        w_ctar1         = rec[8]
        w_cpod          = rec[9]
        w_stato         = rec[10]
        w_segcliente    = rec[11]
        w_id_fornitura  = rec[12]
        w_cdsti         = rec[13]
        w_cflg4         = rec[14]
        w_abi           = rec[15]
        w_cab           = rec[16]
        w_cin           = rec[17]
        w_iban          = rec[18]
        w_d_ini_contr   = rec[19]
        w_prodotto      = rec[20]
        doc['cliente']  = w_acdan
        doc['ragsoc']   = w_ragsoc
        doc['tippers']  = w_tippers
        doc['pr']       = w_cncon
        doc['servizio'] = w_cdservizio
        doc['entita_fatturabile'] = w_cflg9
        doc['tipoiva']  = w_caliv
        doc['tipopag']  = w_cmepag
        doc['versione'] = w_ctar1
        doc['pod']      = w_cpod
        doc['stato']    = w_stato
        doc['segm_cliente'] = w_segcliente
        doc['id_fornitura'] = w_id_fornitura
        doc['dt_attivazione'] = w_d_ini_contr
        doc['per_fatturazione'] = w_cflg4
        doc['abi']          = w_abi
        doc['cab']          = w_cab
        doc['cin']          = w_cin
        doc['iban']         = w_iban
        doc['prodotto']     = w_prodotto
        #
        doc['indirizzi']    = estrai_indirizzi(w_cncon, w_cflg9, cur1, w_acdan)
        doc['periodi']      = estrai_periodi(w_cncon, cur1, w_id_fornitura)
        doc['misuratori']   = estrai_misuratori(w_cncon, cur1)
        #doc['letture']      = estrai_letture(w_id_fornitura, cur1)
        try:
            dbm.pr.insert(doc, safe=True)
        except:
            print "errore insert "
            print doc
if __name__ == "__main__":
    main()